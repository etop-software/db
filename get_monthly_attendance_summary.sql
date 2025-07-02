CREATE OR REPLACE FUNCTION public.get_monthly_attendance_summary(
    p_start_date date DEFAULT NULL::date,
    p_end_date date DEFAULT NULL::date,
    p_employee_id text DEFAULT NULL::text,
    p_search_term text DEFAULT NULL::text
)
    RETURNS TABLE(
        employee_id text, 
        name text,
        "01" text, "02" text, "03" text, "04" text, "05" text, "06" text, "07" text, "08" text, "09" text, "10" text,
        "11" text, "12" text, "13" text, "14" text, "15" text, "16" text, "17" text, "18" text, "19" text, "20" text,
        "21" text, "22" text, "23" text, "24" text, "25" text, "26" text, "27" text, "28" text, "29" text, "30" text, "31" text,
        total_hours text,
        total_absent integer,
        total_leave integer
    ) 
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000
AS $BODY$
WITH employee_attendance AS (
    SELECT 
        s."EMPID" AS employee_id,
        e.id,
        e.name,
        s."PDATE"::date AS shift_date,
        EXTRACT(DAY FROM s."PDATE"::date) AS day_number,
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
            WHEN COUNT(a.datetime) > 1 THEN
                GREATEST(
                    EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) - 
                    (CASE WHEN COALESCE(sh.deduct_break, false) THEN COALESCE(sh.break_time, 0) * 60 ELSE 0 END),
                    0
                )
            ELSE 0
        END AS duration_seconds,
        CASE
            WHEN s."SHIFT" = 'HOLIDAY' THEN 'H'
            WHEN s."SHIFT" = 'WEEKLY OFF' THEN 'W'
            WHEN COUNT(a.datetime) = 0 THEN 'A'
            WHEN COUNT(a.datetime) = 1 THEN 'MP'
            WHEN COUNT(a.datetime) > 1 THEN 'P'
            ELSE '-'
        END AS status_code
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
    LEFT JOIN shifts sh ON TRIM(UPPER(s."SHIFT")) = TRIM(UPPER(sh.shift_code))
    WHERE (p_employee_id IS NULL OR s."EMPID" = p_employee_id)
        AND s."PDATE"::date BETWEEN 
            COALESCE(p_start_date, DATE_TRUNC('month', CURRENT_DATE)) 
            AND COALESCE(p_end_date, DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')
        AND (p_search_term IS NULL OR e.name ILIKE '%' || p_search_term || '%' OR e.employee_id::text ILIKE '%' || p_search_term || '%')
    GROUP BY s."EMPID", e.id, e.name, s."PDATE", s."SHIFT", s."INPUNCH", s."OUTPUNCH", sh.deduct_break, sh.break_time
),
leave_status AS (
    SELECT
        employee_id,
        leave_start_date,
        leave_end_date,
        leave_type_id
    FROM public.employee_leaves
    WHERE leave_start_date <= COALESCE(p_end_date, DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month' - INTERVAL '1 day')
        AND leave_end_date >= COALESCE(p_start_date, DATE_TRUNC('month', CURRENT_DATE))
),
daily_status AS (
    SELECT
        ea.employee_id,
        ea.name,
        ea.day_number,
        ea.shift_date,
        ea.status_code,
        ea.duration_seconds,
        CASE
            WHEN ls.employee_id IS NOT NULL THEN 'Leave'
            WHEN ea.status_code = 'H' THEN 'Holiday'
            WHEN ea.status_code = 'W' THEN 'Weekly off'
            WHEN ea.status_code = 'A' THEN 'Absent'
            WHEN ea.status_code = 'MP' THEN 'Missing punch'
            WHEN ea.status_code = 'P' THEN
                LPAD(FLOOR(ea.duration_seconds / 3600)::TEXT, 2, '0') || ':' || LPAD(FLOOR((ea.duration_seconds % 3600) / 60)::TEXT, 2, '0')
            ELSE '-'
        END AS display_value
    FROM employee_attendance ea
    LEFT JOIN leave_status ls ON ea.id::integer = ls.employee_id
        AND ea.shift_date BETWEEN ls.leave_start_date AND ls.leave_end_date
),
employee_list AS (
    SELECT DISTINCT 
        employee_id,
        name
    FROM daily_status
),
pivoted_data AS (
    SELECT 
        el.employee_id,
        el.name,
        MAX(CASE WHEN ds.day_number = 1 THEN ds.display_value END) AS "01",
        MAX(CASE WHEN ds.day_number = 2 THEN ds.display_value END) AS "02",
        MAX(CASE WHEN ds.day_number = 3 THEN ds.display_value END) AS "03",
        MAX(CASE WHEN ds.day_number = 4 THEN ds.display_value END) AS "04",
        MAX(CASE WHEN ds.day_number = 5 THEN ds.display_value END) AS "05",
        MAX(CASE WHEN ds.day_number = 6 THEN ds.display_value END) AS "06",
        MAX(CASE WHEN ds.day_number = 7 THEN ds.display_value END) AS "07",
        MAX(CASE WHEN ds.day_number = 8 THEN ds.display_value END) AS "08",
        MAX(CASE WHEN ds.day_number = 9 THEN ds.display_value END) AS "09",
        MAX(CASE WHEN ds.day_number = 10 THEN ds.display_value END) AS "10",
        MAX(CASE WHEN ds.day_number = 11 THEN ds.display_value END) AS "11",
        MAX(CASE WHEN ds.day_number = 12 THEN ds.display_value END) AS "12",
        MAX(CASE WHEN ds.day_number = 13 THEN ds.display_value END) AS "13",
        MAX(CASE WHEN ds.day_number = 14 THEN ds.display_value END) AS "14",
        MAX(CASE WHEN ds.day_number = 15 THEN ds.display_value END) AS "15",
        MAX(CASE WHEN ds.day_number = 16 THEN ds.display_value END) AS "16",
        MAX(CASE WHEN ds.day_number = 17 THEN ds.display_value END) AS "17",
        MAX(CASE WHEN ds.day_number = 18 THEN ds.display_value END) AS "18",
        MAX(CASE WHEN ds.day_number = 19 THEN ds.display_value END) AS "19",
        MAX(CASE WHEN ds.day_number = 20 THEN ds.display_value END) AS "20",
        MAX(CASE WHEN ds.day_number = 21 THEN ds.display_value END) AS "21",
        MAX(CASE WHEN ds.day_number = 22 THEN ds.display_value END) AS "22",
        MAX(CASE WHEN ds.day_number = 23 THEN ds.display_value END) AS "23",
        MAX(CASE WHEN ds.day_number = 24 THEN ds.display_value END) AS "24",
        MAX(CASE WHEN ds.day_number = 25 THEN ds.display_value END) AS "25",
        MAX(CASE WHEN ds.day_number = 26 THEN ds.display_value END) AS "26",
        MAX(CASE WHEN ds.day_number = 27 THEN ds.display_value END) AS "27",
        MAX(CASE WHEN ds.day_number = 28 THEN ds.display_value END) AS "28",
        MAX(CASE WHEN ds.day_number = 29 THEN ds.display_value END) AS "29",
        MAX(CASE WHEN ds.day_number = 30 THEN ds.display_value END) AS "30",
        MAX(CASE WHEN ds.day_number = 31 THEN ds.display_value END) AS "31",
      FLOOR(SUM(ds.duration_seconds) / 3600)::TEXT || ':' ||
        LPAD(FLOOR((SUM(ds.duration_seconds) % 3600) / 60)::TEXT, 2, '0') AS total_hours,
        SUM(CASE WHEN ds.display_value = 'Absent' THEN 1 ELSE 0 END) AS total_absent,
        SUM(CASE WHEN ds.display_value = 'Leave' THEN 1 ELSE 0 END) AS total_leave
    FROM employee_list el
    LEFT JOIN daily_status ds ON el.employee_id = ds.employee_id
    GROUP BY el.employee_id, el.name
)
SELECT 
    employee_id,
    name,
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
    total_hours,
    total_absent,
    total_leave
FROM pivoted_data
ORDER BY employee_id;
$BODY$;

ALTER FUNCTION public.get_monthly_attendance_summary(date, date, text, text)
    OWNER TO postgres;
	--SELECT * FROM get_monthly_attendance_summary('2025-06-01', '2025-06-30');
