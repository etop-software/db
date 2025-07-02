CREATE OR REPLACE FUNCTION public.get_monthly_attendance_summary_v1432(
    p_start_date date DEFAULT NULL::date,
    p_end_date date DEFAULT NULL::date,
    p_employee_id text DEFAULT NULL::text,
    p_search_term text DEFAULT NULL::text,
    p_limit integer DEFAULT 50,
    p_offset integer DEFAULT 0)
RETURNS TABLE(
    employee_id text,
    name text,
    daily_attendance jsonb,
    total_hours text,
    total_absent integer,
    total_leave integer,
    total_count integer
)
LANGUAGE 'sql'
COST 100
VOLATILE PARALLEL UNSAFE
ROWS 1000
AS
$BODY$
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
        sh.deduct_break,
        sh.break_time,
        (CASE WHEN COALESCE(sh.deduct_break, false) THEN COALESCE(sh.break_time, 0) * 60 ELSE 0 END) AS break_seconds,
       (CASE 
    WHEN COUNT(a.datetime) > 1 THEN
        GREATEST(
            EXTRACT(EPOCH FROM MAX(a.datetime) - MIN(a.datetime)) - 
            (CASE WHEN COALESCE(sh.deduct_break, false) THEN COALESCE(sh.break_time, 0) * 60 ELSE 0 END),
            0
        )
    WHEN COUNT(a.datetime) = 1 AND s."SHIFT" IN ('WEEKLY OFF', 'HOLIDAY') THEN 
        0
    ELSE 0
END) AS duration_after_break_seconds,
       CASE
    WHEN s."SHIFT" = 'HOLIDAY' AND COUNT(a.datetime) = 0 THEN 'H'
    WHEN s."SHIFT" = 'WEEKLY OFF' AND COUNT(a.datetime) = 0 THEN 'W'
    WHEN COUNT(a.datetime) = 0 THEN 'A'
    WHEN COUNT(a.datetime) = 1 AND s."SHIFT" NOT IN ('HOLIDAY', 'WEEKLY OFF') THEN 'MP'
    WHEN COUNT(a.datetime) >= 1 THEN 'P'
    ELSE '-'
END AS status_code
    FROM tbl_shift_schedule s
    JOIN employees e ON e.employee_id::text = s."EMPID"
    LEFT JOIN attendance a ON a.employee_id::text = s."EMPID"
        AND (
            (s."SHIFT" IN ('HOLIDAY', 'WEEKLY OFF') AND 
             a.datetime BETWEEN s."PDATE"::date::timestamp AND s."PDATE"::date::timestamp + INTERVAL '1 day' - INTERVAL '1 second')
            OR
            (s."SHIFT" NOT IN ('HOLIDAY', 'WEEKLY OFF') AND s."INPUNCH" IS NOT NULL AND s."OUTPUNCH" IS NOT NULL AND
             a.datetime BETWEEN s."INPUNCH" - INTERVAL '5 hour' AND s."OUTPUNCH" + INTERVAL '5 hour')
        )
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
        ea.duration_after_break_seconds,
        CASE
            WHEN ls.employee_id IS NOT NULL THEN 'L'
            WHEN ea.status_code IN ('H', 'W') AND ea.duration_after_break_seconds = 0 THEN ea.status_code
            WHEN ea.status_code = 'A' THEN 'A'
            WHEN ea.status_code = 'MP' THEN 'MP'
            WHEN ea.status_code IN ('P', 'H', 'W') AND ea.duration_after_break_seconds > 0 THEN
                LPAD(FLOOR(ea.duration_after_break_seconds / 3600)::TEXT, 2, '0') || ':' || LPAD(FLOOR((ea.duration_after_break_seconds % 3600) / 60)::TEXT, 2, '0')
            ELSE '-'
        END AS display_value,
        CASE
            WHEN ls.employee_id IS NOT NULL THEN 0
            ELSE ea.duration_after_break_seconds
        END AS effective_duration_seconds
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
aggregated_data AS (
    SELECT 
        el.employee_id,
        el.name,
        jsonb_object_agg(ds.shift_date::text, ds.display_value ORDER BY ds.shift_date) AS daily_attendance,
        FLOOR(SUM(ds.effective_duration_seconds) / 3600)::TEXT || ':' ||
        LPAD(FLOOR((SUM(ds.effective_duration_seconds) % 3600) / 60)::TEXT, 2, '0') AS total_hours,
        SUM(CASE WHEN ds.display_value = 'A' THEN 1 ELSE 0 END) AS total_absent,
        SUM(CASE WHEN ds.display_value = 'L' THEN 1 ELSE 0 END) AS total_leave
    FROM employee_list el
    LEFT JOIN daily_status ds ON el.employee_id = ds.employee_id
    GROUP BY el.employee_id, el.name
),
final_result AS (
    SELECT *,
           COUNT(*) OVER () AS total_count
    FROM aggregated_data
)
SELECT 
    employee_id,
    name,
    daily_attendance,
    total_hours,
    total_absent,
    total_leave,
    total_count
FROM final_result
ORDER BY employee_id
LIMIT p_limit OFFSET p_offset;
$BODY$;

ALTER FUNCTION public.get_monthly_attendance_summary_v1432(date, date, text, text, integer, integer)
OWNER TO postgres;
select * from get_monthly_attendance_summary_v1432('2025-05-15','2025-06-15')