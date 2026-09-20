#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/SetSettings.h>
#include <Storages/StorageSet.h>
#include <Storages/StorageFactory.h>
#include <Compression/CompressedReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Formats/NativeWriter.h>
#include <Formats/NativeReader.h>
#include <QueryPipeline/ProfileInfo.h>
#include <Disks/IDisk.h>
#include <Common/CurrentThread.h>
#include <Common/formatReadable.h>
#include <Common/StringUtils.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/logger_useful.h>
#include <Interpreters/Set.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Parsers/ASTCreateQuery.h>
#include <Backups/BackupEntriesCollector.h>
#include <Backups/BackupEntryFromAppendOnlyFile.h>
#include <Backups/BackupEntryFromMemory.h>
#include <Backups/BackupEntryWrappedWith.h>
#include <Backups/IBackup.h>
#include <Backups/RestorerFromBackup.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Disks/WriteMode.h>
#include <algorithm>
#include <filesystem>
#include <optional>
#include <vector>

namespace fs = std::filesystem;


namespace DB
{

namespace SetSetting
{
    extern const SetSettingsString disk;
    extern const SetSettingsBool persistent;
}

namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
    extern const int INCORRECT_FILE_NAME;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
/// Written to every backup of a Set or Join table, even when the table produced no data
/// files, so that restore can tell a real (possibly empty) backup apart from a
/// structure_only or pre-fix metadata-only one.
const char * set_or_join_backup_marker = ".set_or_join_backup";
}

class SetOrJoinSink final : public SinkToStorage, WithContext
{
public:
    SetOrJoinSink(
        ContextPtr ctx, StorageSetOrJoinBase & table_, const StorageMetadataPtr & metadata_snapshot_,
        const String & backup_path_, const String & backup_tmp_path_,
        const String & backup_file_name_, bool persistent_);
    ~SetOrJoinSink() override;

    String getName() const override { return "SetOrJoinSink"; }
    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    void cancelBuffers() noexcept;

    StorageSetOrJoinBase & table;
    StorageMetadataPtr metadata_snapshot;
    String backup_path;
    String backup_tmp_path;
    String backup_file_name;
    std::unique_ptr<WriteBufferFromFileBase> backup_buf;
    std::optional<CompressedWriteBuffer> compressed_backup_buf;
    std::optional<NativeWriter> backup_stream;
    bool persistent;
};


SetOrJoinSink::SetOrJoinSink(
    ContextPtr ctx,
    StorageSetOrJoinBase & table_,
    const StorageMetadataPtr & metadata_snapshot_,
    const String & backup_path_,
    const String & backup_tmp_path_,
    const String & backup_file_name_,
    bool persistent_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , WithContext(ctx)
    , table(table_)
    , metadata_snapshot(metadata_snapshot_)
    , backup_path(backup_path_)
    , backup_tmp_path(backup_tmp_path_)
    , backup_file_name(backup_file_name_)
    , persistent(persistent_)
{
}

SetOrJoinSink::~SetOrJoinSink()
{
    if (isCancelled())
        cancelBuffers();
}

void SetOrJoinSink::cancelBuffers() noexcept
{
    if (compressed_backup_buf)
        compressed_backup_buf->cancel();
    if (backup_buf)
        backup_buf->cancel();
}


void SetOrJoinSink::consume(Chunk & chunk)
{
    Block block = getHeader().cloneWithColumns(chunk.getColumns());

    table.insertBlock(block, getContext());
    if (persistent)
    {
        if (!backup_buf)
        {
            backup_buf = table.disk->writeFile(fs::path(backup_tmp_path) / backup_file_name);
            compressed_backup_buf.emplace(*backup_buf);
            backup_stream.emplace(*compressed_backup_buf, 0, std::make_shared<const Block>(metadata_snapshot->getSampleBlock()));
        }
        backup_stream->write(block);
    }
}

void SetOrJoinSink::onFinish()
{
    table.finishInsert();
    if (backup_buf)
    {
        backup_stream->flush();
        compressed_backup_buf->finalize();
        backup_buf->finalize();

        table.disk->replaceFile(fs::path(backup_tmp_path) / backup_file_name, fs::path(backup_path) / backup_file_name);
    }
}


SinkToStoragePtr StorageSetOrJoinBase::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool /*async_insert*/)
{
    UInt64 id = ++increment;
    return std::make_shared<SetOrJoinSink>(
        context, *this, metadata_snapshot, path, fs::path(path) / "tmp/", toString(id) + ".bin", persistent);
}


StorageSetOrJoinBase::StorageSetOrJoinBase(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool persistent_)
    : StorageWithCommonVirtualColumns(table_id_), disk(disk_), persistent(persistent_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);

    if (relative_path_.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Join and Set storages require data path");

    path = relative_path_;
}

VirtualColumnsDescription StorageSetOrJoinBase::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


StorageSet::StorageSet(
    DiskPtr disk_,
    const String & relative_path_,
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    bool persistent_)
    : StorageSetOrJoinBase{disk_, relative_path_, table_id_, columns_, constraints_, comment, persistent_}
    , set(std::make_shared<Set>(SizeLimits(), 0, true))
{
    auto metadata_snapshot = getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    Block header = metadata_snapshot->getSampleBlock();
    set->setHeader(header.getColumnsWithTypeAndName());

    restore();
}


SetPtr StorageSet::getSet() const
{
    std::lock_guard lock(mutex);
    return set;
}


void StorageSet::insertBlock(const Block & block, ContextPtr)
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    current_set->insertFromBlock(block.getColumnsWithTypeAndName());
}

void StorageSet::finishInsert()
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    current_set->finishInsert();
}

size_t StorageSet::getSize(ContextPtr) const
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    return current_set->getTotalRowCount();
}

std::optional<UInt64> StorageSet::totalRows(ContextPtr) const
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    return current_set->getTotalRowCount();
}

std::optional<UInt64> StorageSet::totalBytes(ContextPtr) const
{
    SetPtr current_set;
    {
        std::lock_guard lock(mutex);
        current_set = set;
    }
    return current_set->getTotalByteCount();
}

void StorageSet::truncate(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr, TableExclusiveLockHolder &)
{
    if (disk->existsDirectory(path))
        disk->removeRecursive(path);
    else
        LOG_INFO(getLogger("StorageSet"), "Path {} is already removed from disk {}", path, disk->getName());

    disk->createDirectories(path);
    disk->createDirectories(fs::path(path) / "tmp/");

    Block header = metadata_snapshot->getSampleBlock();

    increment = 0;

    auto new_set = std::make_shared<Set>(SizeLimits(), 0, true);
    new_set->setHeader(header.getColumnsWithTypeAndName());
    {
        std::lock_guard lock(mutex);
        set = new_set;
    }
}


void StorageSetOrJoinBase::restore()
{
    if (!disk->existsDirectory(fs::path(path) / "tmp"))
    {
        disk->createDirectories(fs::path(path) / "tmp");
        return;
    }

    static const char * file_suffix = ".bin";
    static const auto file_suffix_size = strlen(".bin");

    using FilePriority = std::pair<UInt64, String>;
    std::priority_queue<FilePriority, std::vector<FilePriority>, std::greater<>> backup_files;
    for (auto dir_it{disk->iterateDirectory(path)}; dir_it->isValid(); dir_it->next())
    {
        const auto & name = dir_it->name();
        const auto & file_path = dir_it->path();

        if (disk->existsFile(file_path)
            && endsWith(name, file_suffix)
            && disk->getFileSize(file_path) > 0)
        {
            /// Calculate the maximum number of available files with a backup to add the following files with large numbers.
            UInt64 file_num = parse<UInt64>(name.substr(0, name.size() - file_suffix_size));
            if (file_num > increment)
                increment = file_num;

            backup_files.push({file_num, file_path});
        }
    }

    /// Restore in the same order as blocks were written
    /// It may be important for storage Join, user expect to get the first row (unless `join_any_take_last_row` setting is set)
    /// but after restart we may have different order of blocks in memory.
    while (!backup_files.empty())
    {
        restoreFromFile(backup_files.top().second);
        backup_files.pop();
    }
}


void StorageSetOrJoinBase::restoreFromFile(const String & file_path)
{
    ContextPtr ctx = nullptr;
    auto backup_buf = disk->readFile(file_path, getReadSettings());
    CompressedReadBuffer compressed_backup_buf(*backup_buf);
    NativeReader backup_stream(compressed_backup_buf, 0);

    ProfileInfo info;
    for (Block block = backup_stream.read(); !block.empty(); block = backup_stream.read())
    {
        info.update(block);
        insertBlock(block, ctx);
    }

    finishInsert();

    /// TODO Add speed, compressed bytes, data volume in memory, compression ratio ... Generalize all statistics logging in project.
    LOG_INFO(getLogger("StorageSetOrJoinBase"), "Loaded from backup file {}. {} rows, {}. State has {} unique rows.",
        file_path, info.rows, ReadableSize(info.bytes), getSize(ctx));
}


void StorageSetOrJoinBase::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    fs::path data_path_in_backup_fs = data_path_in_backup;

    size_t num_files = 0;
    if (persistent && disk->existsDirectory(path))
    {
        /// Each finished data file is immutable once SetOrJoinSink::onFinish() moves it out of
        /// tmp/, but a concurrent TRUNCATE removes the whole data directory. Hard-link every file
        /// into a private temporary directory first so the backup entries stay valid regardless
        /// (the same approach StorageLog uses for its data files).
        auto temp_dir_owner = std::make_shared<TemporaryFileOnDisk>(disk, "tmp/");
        fs::path temp_dir = temp_dir_owner->getRelativePath();
        disk->createDirectories(temp_dir);

        const auto & backup_settings = backup_entries_collector.getBackupSettings();
        bool copy_encrypted = !backup_settings.decrypt_files_from_encrypted_disks;
        bool allow_checksums_from_remote_paths = backup_settings.allow_checksums_from_remote_paths;

        for (auto dir_it = disk->iterateDirectory(path); dir_it->isValid(); dir_it->next())
        {
            const auto & file_name = dir_it->name();
            const auto & file_path = dir_it->path();

            /// The tmp/ subdirectory and still-in-flight inserts (written under tmp/ and moved
            /// into place atomically) are not part of the durable state yet, so only the
            /// finalized *.bin files are copied.
            if (!disk->existsFile(file_path) || !endsWith(file_name, ".bin"))
                continue;
            auto file_size = disk->getFileSize(file_path);
            if (!file_size)
                continue;

            String hardlink_file_path = temp_dir / file_name;
            disk->createHardLink(file_path, hardlink_file_path);
            BackupEntryPtr backup_entry = std::make_unique<BackupEntryFromAppendOnlyFile>(
                disk, hardlink_file_path, copy_encrypted, file_size, allow_checksums_from_remote_paths);
            backup_entry = wrapBackupEntryWith(std::move(backup_entry), temp_dir_owner);
            backup_entries_collector.addBackupEntry(data_path_in_backup_fs / file_name, std::move(backup_entry));
            ++num_files;
        }
    }
    /// else: nothing durable exists to save. The in-memory state has no serialization, and the
    /// table was explicitly created with persistent = false, which opts out of surviving a
    /// restart; a backup round-trip is no stronger a guarantee than that.

    /// Always write the marker, even when no data files were produced, so that restore can tell
    /// this backup apart from a structure_only or pre-fix metadata-only backup.
    backup_entries_collector.addBackupEntry(
        data_path_in_backup_fs / set_or_join_backup_marker,
        std::make_unique<BackupEntryFromMemory>(toString(num_files)));
}

void StorageSetOrJoinBase::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & /* partitions */)
{
    auto backup = restorer.getBackup();

    /// backupData() writes the marker file even for an empty table, so a data restore finding no
    /// marker means one of: (a) the backup was made with structure_only = true and should be
    /// restored the same way, with SETTINGS structure_only = true; (b) the backup predates
    /// Set/Join tables backing up their data
    /// (see https://github.com/ClickHouse/ClickHouse/issues/121176); or (c) the backup is
    /// corrupted. Restoring it as data would silently recreate an empty table, so fail closed.
    String marker_file = fs::path(data_path_in_backup) / set_or_join_backup_marker;
    if (!backup->fileExists(marker_file))
        throw Exception(
            ErrorCodes::CANNOT_RESTORE_TABLE,
            "Backup of table {} has no data marker {}. If this backup was created with structure_only = true, "
            "restore it with SETTINGS structure_only = true. Otherwise it predates Set/Join tables backing up "
            "their data (see https://github.com/ClickHouse/ClickHouse/issues/121176) or is corrupted; restoring "
            "its data would silently produce an empty table",
            getStorageID().getNameForLogs(), marker_file);

    if (!restorer.isNonEmptyTableAllowed() && getSize(restorer.getContext()))
        RestorerFromBackup::throwTableIsNotEmpty(getStorageID());

    restorer.addDataRestoreTask(
        [storage = std::static_pointer_cast<StorageSetOrJoinBase>(shared_from_this()), backup, data_path_in_backup]
        { storage->restoreDataImpl(backup, data_path_in_backup); });
}

void StorageSetOrJoinBase::restoreDataImpl(const BackupPtr & backup, const String & data_path_in_backup)
{
    fs::path data_path_in_backup_fs = data_path_in_backup;

    /// Collect the backed-up data files in the order the blocks were originally written.
    /// The order may be important for storage Join, where the user expects to get the first
    /// row (unless `join_any_take_last_row` is set).
    std::vector<std::pair<UInt64, String>> data_files;
    static const char * file_suffix = ".bin";
    static const auto file_suffix_size = strlen(".bin");
    for (const String & file_name : backup->listFiles(data_path_in_backup, /* recursive */ false))
    {
        if (!endsWith(file_name, file_suffix))
            continue;
        UInt64 file_num = parse<UInt64>(file_name.substr(0, file_name.size() - file_suffix_size));
        data_files.emplace_back(file_num, file_name);
    }
    std::sort(data_files.begin(), data_files.end());

    for (const auto & data_file : data_files)
    {
        String file_path_in_backup = data_path_in_backup_fs / data_file.second;
        if (persistent)
        {
            /// Land the file under a fresh number so that a restore into a non-empty table
            /// (allow_non_empty_tables) cannot collide with its existing files, then replay it.
            String target_path = fs::path(path) / (toString(++increment) + file_suffix);
            backup->copyFileToDisk(file_path_in_backup, disk, target_path, WriteMode::Rewrite, /* sync= */ false);
            restoreFromFile(target_path);
        }
        else
        {
            /// A non-persistent table keeps nothing on disk; load the rows straight into memory.
            auto read_buf = backup->readFile(file_path_in_backup);
            CompressedReadBuffer compressed_buf(*read_buf);
            NativeReader reader(compressed_buf, 0);
            for (Block block = reader.read(); !block.empty(); block = reader.read())
                insertBlock(block, nullptr);
            finishInsert();
        }
    }
}


void StorageSetOrJoinBase::rename(const String & new_path_to_table_data, const StorageID & new_table_id)
{
    /// Rename directory with data.
    disk->replaceFile(path, new_path_to_table_data);

    path = new_path_to_table_data;
    renameInMemory(new_table_id);
}


void registerStorageSet(StorageFactory & factory);
void registerStorageSet(StorageFactory & factory)
{
    factory.registerStorage("Set", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        bool has_settings = args.storage_def->settings;
        SetSettings set_settings;
        if (has_settings)
            set_settings.loadFromQuery(*args.storage_def);

        DiskPtr disk = args.getContext()->getDisk(set_settings[SetSetting::disk]);
        return std::make_shared<StorageSet>(
            disk, args.relative_data_path, args.table_id, args.columns, args.constraints, args.comment, set_settings[SetSetting::persistent]);
    }, StorageFactory::StorageFeatures{ .supports_settings = true, .has_builtin_setting_fn = SetSettings::hasBuiltin, },
    Documentation{
        .description = R"DOCS_MD(
<Note>
In ClickHouse Cloud, if your service was created with a version earlier than 25.4, you will need to set the compatibility to at least 25.4 using  `SET compatibility=25.4`.
</Note>

A data set that is always in RAM. It is intended for use on the right side of the `IN` operator (see the section "IN operators").

You can use `INSERT` to insert data in the table. New elements will be added to the data set, while duplicates will be ignored.
But you can't perform `SELECT` from the table. The only way to retrieve data is by using it in the right half of the `IN` operator.

Data is always located in RAM. For `INSERT`, the blocks of inserted data are also written to the directory of tables on the disk. When starting the server, this data is loaded to RAM. In other words, after restarting, the data remains in place.

For a rough server restart, the block of data on the disk might be lost or damaged. In the latter case, you may need to manually delete the file with damaged data.

### Limitations and settings {#join-limitations-and-settings}

When creating a table, the following settings are applied:

#### Persistent {#persistent}

Disables persistency for the Set and [Join](/reference/engines/table-engines/special/join) table engines.

Reduces the I/O overhead. Suitable for scenarios that pursue performance and do not require persistence.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.
)DOCS_MD",
        .syntax = "ENGINE = Set",
        .related = {"Join"}});
}


}
