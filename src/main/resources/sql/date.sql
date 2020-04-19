DEBUG ON;

OPEN QROSS;

SET $a := 1;
SET $b :=
    IF $a == 1 THEN
        SELECT id, title FROM qross_jobs LIMIT 2
    ELSIF $a > 1 THEN
        SELECT id, title FROM qross_jobs WHERE id>100 LIMIT 2
    ELSE
        SELECT id, title FROM qross_jobs WHERE id>200 LIMIT 2
    END;

PRINT $b;

FOR $a, $b IN {"name": "Tom", "age": 18 } LOOP
    PRINT $a + ":" + $b;
END LOOP;

SET $t1 := SELECT id FROM qross_jobs LIMIT 0 -> IF NULL '1';
SET $t2 := SELECT id FROM qross_jobs LIMIT 0 -> IF EMPTY '2';
SET $t3 := SELECT id FROM qross_jobs LIMIT 0;

PRINT $t1;
PRINT $t2;
PRINT $t3;


