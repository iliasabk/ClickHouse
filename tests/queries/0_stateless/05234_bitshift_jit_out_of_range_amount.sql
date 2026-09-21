-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/121305
--
-- JIT-compiled bitShiftLeft/bitShiftRight used to emit a raw LLVM
-- shl/lshr/ashr, which is poison when the shift amount is >= the bit width
-- of the shifted type. When fused with neighbouring compiled expressions,
-- LLVM was free to fold the poison value into wrong results. The
-- interpreter returns 0 for such amounts (and the compiled expression now
-- does too).

SELECT 'shift amount >= bit width, jit off';
SELECT
    bitAnd(bitShiftRight(v8, 63), 1) = 0,
    bitShiftRight(v8, 8),
    bitShiftRight(v8, 256),
    bitShiftLeft(v8, 8),
    bitShiftLeft(v8, 300),
    bitShiftRight(v16, 16),
    bitShiftLeft(v64, 64),
    bitShiftRight(s8, 9)
FROM
(
    SELECT
        materialize(toUInt8(255)) AS v8,
        materialize(toUInt16(65535)) AS v16,
        materialize(toUInt64(1)) AS v64,
        materialize(toInt8(-16)) AS s8
)
SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;

SELECT 'shift amount >= bit width, jit on';
SELECT
    bitAnd(bitShiftRight(v8, 63), 1) = 0,
    bitShiftRight(v8, 8),
    bitShiftRight(v8, 256),
    bitShiftLeft(v8, 8),
    bitShiftLeft(v8, 300),
    bitShiftRight(v16, 16),
    bitShiftLeft(v64, 64),
    bitShiftRight(s8, 9)
FROM
(
    SELECT
        materialize(toUInt8(255)) AS v8,
        materialize(toUInt16(65535)) AS v16,
        materialize(toUInt64(1)) AS v64,
        materialize(toInt8(-16)) AS s8
)
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

SELECT 'in-range shifts, jit off';
SELECT
    bitShiftLeft(v8, 4),
    bitShiftRight(v16, 4),
    bitShiftRight(v64, 63),
    bitShiftLeft(v8, 0),
    bitShiftRight(s8, 3)
FROM
(
    SELECT
        materialize(toUInt8(255)) AS v8,
        materialize(toUInt16(65535)) AS v16,
        materialize(toUInt64(9223372036854775808)) AS v64,
        materialize(toInt8(-16)) AS s8
)
SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;

SELECT 'in-range shifts, jit on';
SELECT
    bitShiftLeft(v8, 4),
    bitShiftRight(v16, 4),
    bitShiftRight(v64, 63),
    bitShiftLeft(v8, 0),
    bitShiftRight(s8, 3)
FROM
(
    SELECT
        materialize(toUInt8(255)) AS v8,
        materialize(toUInt16(65535)) AS v16,
        materialize(toUInt64(9223372036854775808)) AS v64,
        materialize(toInt8(-16)) AS s8
)
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;