-- FUNCTION: public.get_attendance_report_21_06(text, date, date, integer, integer, text, text)

-- DROP FUNCTION IF EXISTS public.get_attendance_report_21_06(text, date, date, integer, integer, text, text);

CREATE OR REPLACE FUNCTION public.get_attendance_report_21_06(
	p_employee_id text DEFAULT NULL::text,
	p_start_date date DEFAULT NULL::date,
	p_end_date date DEFAULT NULL::date,
	p_limit integer DEFAULT NULL::integer,
	p_offset integer DEFAULT NULL::integer,
	p_search_term text DEFAULT NULL::text,
	p_filter_by text DEFAULT NULL::text)
    RETURNS TABLE(employee_id text, name text, shift_date date, "SHIFT" text, expected_in timestamp without time zone, expected_out timestamp without time zone, actual_in timestamp without time zone, actual_out timestamp without time zone, punch_count integer, duration text, status text, regular_ot text, holiday_ot text, weekly_off_ot text, total_ot text, total_records bigint) 
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
WITH raw_attendance AS (
    SELECT 
        s."EMPID" AS employee_id,
        e.id,
        e.name,
        s."PDATE"::date AS shift_date,
        s."SHIFT",
        s."INPUNCH" AS expected_in,
        s."OUTPUNCH" AS expected_out,
        COUNT(a.datetime) AS punch_count,
        CASE 
            WHEN COUNT(a.datetime) = 0 THEN NULL
            WHEN COUNT(a.datetime) = 1 THEN
                CASE 
                    WHEN ABS(EXTRACT(EPOCH FROM MIN(a.datetime) - s."INPUNCH")) <= 
                         ABS(EXTRACT(EPOCH FROM MIN(a.datetime) - s."OUTPUNCH")) 
                    THEN MIN(a.datetime)
                    ELSE NULL
                END
            ELSE MIN(a.datetime)
        END AS actual_in,
        CASE 
            WHEN COUNT(a.datetime) = 0 THEN NULL
            WHEN COUNT(a.datetime) = 1 THEN
                CASE 
                    WHEN ABS(EXTRACT(EPOCH FROM MIN(a.datetime) - s."OUTPUNCH")) < 
                         ABS(EXTRACT(EPOCH FROM MIN(a.datetime) - s."INPUNCH")) 
                    THEN MIN(a.datetime)
                    ELSE NULL
                END
            ELSE MAX(a.datetime)
        END AS actual_out,
     CASE 
    WHEN COUNT(a.datetime) <= 1 THEN NULL  
    ELSE ROUND(
        GREATEST(
            EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) - 
            (CASE WHEN COALESCE(sh.deduct_break, false) THEN COALESCE(sh.break_time, 0) * 60 ELSE 0 END),
            0
        ) / 3600.0, 
    2)
END AS duration_hours,

CASE 
    WHEN COUNT(a.datetime) <= 1 THEN NULL  
    ELSE 
        LPAD(FLOOR(
            GREATEST(
                EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) - 
                (CASE WHEN COALESCE(sh.deduct_break, false) THEN COALESCE(sh.break_time, 0) * 60 ELSE 0 END),
                0
            ) / 3600
        )::TEXT, 2, '0') || ':' ||
        LPAD(FLOOR(
            (GREATEST(
                EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) - 
                (CASE WHEN COALESCE(sh.deduct_break, false) THEN COALESCE(sh.break_time, 0) * 60 ELSE 0 END),
                0
            ) % 3600) / 60
        )::TEXT, 2, '0')
END AS duration_formatted,

        CASE 
            WHEN s."SHIFT" NOT IN ('HOLIDAY', 'WEEKLY OFF') 
                 AND COUNT(a.datetime) > 1 
                 AND MAX(a.datetime) > s."OUTPUNCH" + (COALESCE(sh.ot_starts_after, 0) * interval '1 minute')
            THEN 
                LPAD(FLOOR(EXTRACT(EPOCH FROM MAX(a.datetime) - (s."OUTPUNCH" + (COALESCE(sh.ot_starts_after, 0) * interval '1 minute'))) / 3600)::text, 2, '0') 
                || ':' ||
                LPAD(FLOOR((EXTRACT(EPOCH FROM MAX(a.datetime) - (s."OUTPUNCH" + (COALESCE(sh.ot_starts_after, 0) * interval '1 minute'))) % 3600) / 60)::text, 2, '0')
            ELSE '00:00'
        END AS regular_ot,
        CASE 
            WHEN s."SHIFT" = 'HOLIDAY' AND COUNT(a.datetime) > 1
            THEN 
                LPAD(FLOOR(EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) / 3600)::text, 2, '0') 
                || ':' ||
                LPAD(FLOOR((EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) % 3600) / 60)::text, 2, '0')
            ELSE '00:00'
        END AS holiday_ot,
        CASE 
            WHEN s."SHIFT" = 'WEEKLY OFF' AND COUNT(a.datetime) > 1
            THEN 
                LPAD(FLOOR(EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) / 3600)::text, 2, '0') 
                || ':' ||
                LPAD(FLOOR((EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) % 3600) / 60)::text, 2, '0')
            ELSE '00:00'
        END AS weekly_off_ot,
        CASE
            WHEN s."SHIFT" = 'HOLIDAY' THEN 'Public Holiday'
            WHEN s."SHIFT" = 'WEEKLY OFF' THEN 'Weekly Off'
            WHEN COUNT(a.datetime) = 0 THEN 'Absent'
            ELSE 'OK'
        END AS preliminary_status
    FROM tbl_shift_schedule s
    JOIN employees e ON e.employee_id::text = s."EMPID"
    LEFT JOIN attendance a ON a.employee_id::text = s."EMPID"
        AND a.datetime BETWEEN
            CASE WHEN s."SHIFT" IN ('HOLIDAY', 'WEEKLY OFF') THEN
                s."PDATE"::date::timestamp
            ELSE
                s."INPUNCH" - INTERVAL '5 hour'
            END
            AND
            CASE WHEN s."SHIFT" IN ('HOLIDAY', 'WEEKLY OFF') THEN
                s."PDATE"::date::timestamp + INTERVAL '1 day' - INTERVAL '1 second'
            ELSE
                s."OUTPUNCH" + INTERVAL '5 hour'
            END
        AND s."PDATE"::date < CURRENT_DATE
    LEFT JOIN shifts sh ON TRIM(UPPER(s."SHIFT")) = TRIM(UPPER(sh.shift_code))
    WHERE (p_employee_id IS NULL OR a.employee_id::text = p_employee_id)
        AND (p_start_date IS NULL OR s."PDATE"::date >= p_start_date)
        AND (p_end_date IS NULL OR s."PDATE"::date <= p_end_date)
        AND (p_search_term IS NULL OR p_filter_by IS NULL OR p_filter_by = 'all' OR
             (p_filter_by = 'employee_id' AND e.employee_id::text ILIKE '%' || p_search_term || '%') OR
             (p_filter_by = 'name' AND e.name ILIKE '%' || p_search_term || '%') OR
             (p_filter_by = 'shift' AND s."SHIFT" ILIKE '%' || p_search_term || '%'))
    GROUP BY a.employee_id, e.name, s."PDATE", s."SHIFT", s."INPUNCH", s."OUTPUNCH", e.id, s."EMPID", sh.ot_starts_after,sh.deduct_break,sh.break_time
),
leave_status AS (
    SELECT
        employee_id,
        leave_start_date,
        leave_end_date,
        leave_type_id
    FROM public.employee_leaves
)
SELECT
    ra.employee_id,
    ra.name,
    ra.shift_date,
    ra."SHIFT",
    ra.expected_in,
    ra.expected_out,
    ra.actual_in,
    ra.actual_out,
    ra.punch_count,
    ra.duration_formatted AS duration,
    CASE
        WHEN ra.preliminary_status IN ('Public Holiday', 'Weekly Off') THEN ra.preliminary_status
        WHEN ra.punch_count = 0 
             AND ls.employee_id IS NOT NULL 
             AND ra.shift_date BETWEEN ls.leave_start_date AND ls.leave_end_date
        THEN 'Leave - ' || l.leave_name
        WHEN ra.punch_count = 0 
             AND (ls.employee_id IS NULL OR ra.shift_date NOT BETWEEN ls.leave_start_date AND ls.leave_end_date)
        THEN 'Absent'
        WHEN ra.punch_count = 1 AND ra.actual_in IS NOT NULL THEN 'Missing punch-out'
        WHEN ra.punch_count = 1 AND ra.actual_out IS NOT NULL THEN 'Missing punch-in'
        WHEN ra.actual_in IS NULL THEN 'Missing punch-in'
        WHEN ra.actual_out IS NULL THEN 'Missing punch-out'
        WHEN ra.duration_hours < 0.2 THEN 'Very short session'
        WHEN ra.duration_hours > 16 THEN 'Very long session'
        ELSE 'OK'
    END AS status,
    ra.regular_ot,
    ra.holiday_ot,
    ra.weekly_off_ot,
    LPAD(
        FLOOR((
            EXTRACT(EPOCH FROM ra.regular_ot::interval) +
            EXTRACT(EPOCH FROM ra.holiday_ot::interval) +
            EXTRACT(EPOCH FROM ra.weekly_off_ot::interval)
        ) / 3600)::TEXT, 2, '0'
    ) || ':' ||
    LPAD(
        FLOOR((
            EXTRACT(EPOCH FROM ra.regular_ot::interval) +
            EXTRACT(EPOCH FROM ra.holiday_ot::interval) +
            EXTRACT(EPOCH FROM ra.weekly_off_ot::interval)
        ) % 3600 / 60)::TEXT, 2, '0'
    ) AS total_ot,
    COUNT(*) OVER () AS total_records
FROM raw_attendance ra
LEFT JOIN leave_status ls ON ra.id::integer = ls.employee_id
    AND ra.shift_date BETWEEN ls.leave_start_date AND ls.leave_end_date
LEFT JOIN leaves l ON ls.leave_type_id = l.id
WHERE (p_search_term IS NULL OR p_filter_by IS NULL OR p_filter_by != 'status' OR
       (p_filter_by = 'status' AND (
           CASE
               WHEN ra.preliminary_status IN ('Public Holiday', 'Weekly Off') THEN ra.preliminary_status
               WHEN ra.punch_count = 0 
                    AND ls.employee_id IS NOT NULL 
                    AND ra.shift_date BETWEEN ls.leave_start_date AND ls.leave_end_date
               THEN 'Leave - ' || l.leave_name
               WHEN ra.punch_count = 0 
                    AND (ls.employee_id IS NULL OR ra.shift_date NOT BETWEEN ls.leave_start_date AND ls.leave_end_date)
               THEN 'Absent'
               WHEN ra.punch_count = 1 AND ra.actual_in IS NOT NULL THEN 'Missing punch-out'
               WHEN ra.punch_count = 1 AND ra.actual_out IS NOT NULL THEN 'Missing punch-in'
               WHEN ra.actual_in IS NULL THEN 'Missing punch-in'
               WHEN ra.actual_out IS NULL THEN 'Missing punch-out'
               WHEN ra.duration_hours < 0.2 THEN 'Very short session'
               WHEN ra.duration_hours > 16 THEN 'Very long session'
               ELSE 'OK'
           END ILIKE '%' || p_search_term || '%'
       ))
)
ORDER BY ra.shift_date, ra.employee_id
LIMIT COALESCE(p_limit, 1000)
OFFSET COALESCE(p_offset, 0);
$BODY$;

ALTER FUNCTION public.get_attendance_report_21_06(text, date, date, integer, integer, text, text)
    OWNER TO postgres;
