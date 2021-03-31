CREATE OR REPLACE VIEW "dimensions"."ticket_sales" AS 
SELECT 
ticketTbl.ttd_loc_id as "Location Code",
CASE 
WHEN ticketTbl.ttd_bus_date LIKE '%-%-%' THEN DATE_PARSE(ticketTbl.ttd_bus_date, '%Y-%m-%d')
WHEN ticketTbl.ttd_bus_date LIKE '%/%/%' THEN DATE_PARSE(ticketTbl.ttd_bus_date, '%d/%m/%Y')
ELSE null END as "Movie Date",
TRY_CAST(ticketTbl.ttd_booking_id AS BIGINT) AS "Booking ID",
TRY_CAST(ticketTbl.ttd_show_id AS BIGINT) AS "Show ID",
ticketTbl.ttd_row_id as "Seat Row",
TRY_CAST(ticketTbl.ttd_col_id AS BIGINT) AS "Seat Column",
ticketTbl.ttd_pos_no as "POS No",
TRY_CAST(ticketTbl.ttd_film_id AS BIGINT) AS "ttd_film_id",
TRY_CAST(ticketTbl.ttd_hall_id AS BIGINT) AS "ttd_hall_id",
CASE
WHEN ticketTbl.ttd_trxn_date LIKE '%-%-%' THEN DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')
ELSE null
END AS "Transaction Date",
TRY_CAST(ticketTbl.ttd_ticket_no AS BIGINT) AS "ttd_ticket_no",
ticketTbl.ttd_tkt_type,
SUM(ticketTbl.ttd_tkt_amt) AS "Ticket Amount",
SUM(ticketTbl.ttd_tkt_gst_amt) AS "Ticket GST Amount",
SUM(ticketTbl.ttd_tkt_ent_tax) AS "Ticket ENT. Tax",
SUM(ticketTbl.ttd_tkt_bkg_fee_amt) AS "Booking Fee Amount",
SUM(ticketTbl.ttd_tkt_bkg_fee_gst_amt) AS "Booking Fee GST Amount",
SUM(ticketTbl.ttd_tkt_surc_amt) AS "Surcharge Amount",
SUM(ticketTbl.ttd_tkt_surc_gst_amt) AS "Surcharge GST Amount",
ticketTbl.ttd_pymt_channel_type,
TRY_CAST(ticketTbl.ttd_show_time AS BIGINT) AS "ttd_show_time",
ticketTbl.ttd_show_date AS "Show Date",
CONCAT(ticketTbl.ttd_row_id, TRY_CAST(TRY_CAST(ticketTbl.ttd_col_id AS BIGINT) AS VARCHAR)) AS "Seat Number",
ROUND(SUM(ticketTbl.ttd_tkt_amt-ticketTbl.ttd_tkt_gst_amt-ticketTbl.ttd_tkt_ent_tax+ticketTbl.ttd_tkt_surc_amt-ticketTbl.ttd_tkt_surc_gst_amt),2) AS "NBO",
ROUND(SUM(ticketTbl.ttd_tkt_amt+ticketTbl.ttd_tkt_surc_amt),2) AS "GBO",
COUNT(DISTINCT ticketTbl.ttd_ticket_no) AS "Admission",
ROUND(SUM(ticketTbl.ttd_tkt_amt-ticketTbl.ttd_tkt_gst_amt-ticketTbl.ttd_tkt_ent_tax),2) AS "Net Ticket Price",
ROUND(SUM(ticketTbl.ttd_tkt_surc_amt-ticketTbl.ttd_tkt_surc_gst_amt),2) AS "Net Surcharge",
ROUND(SUM(ticketTbl.ttd_tkt_bkg_fee_amt-ticketTbl.ttd_tkt_bkg_fee_gst_amt),2) AS "Net Booking Fee",
ROUND(SUM(ticketTbl.ttd_tkt_amt-ticketTbl.ttd_tkt_gst_amt-ticketTbl.ttd_tkt_ent_tax+ticketTbl.ttd_tkt_surc_amt-ticketTbl.ttd_tkt_surc_gst_amt),2)/COUNT(DISTINCT ticketTbl.ttd_ticket_no) AS "ATP",
COUNT(DISTINCT ticketTbl.ttd_booking_id) AS "No of TRX (Ticket)",
ROUND(COUNT(DISTINCT ticketTbl.ttd_ticket_no)/SUM(schedule.totalseats)*100, 2) AS "Occupancy Rate (%)",

CASE
WHEN ticketTbl.ttd_trxn_date LIKE '%-%-%' AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) > 12 AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f'))-12 = 12 THEN CONCAT('0',CONCAT(TRY_CAST(EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f'))-12 AS VARCHAR),':00AM')) 
WHEN ticketTbl.ttd_trxn_date LIKE '%-%-%' AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) > 12 AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f'))-12 < 10 THEN CONCAT('0',CONCAT(TRY_CAST(EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f'))-12 AS VARCHAR),':00PM')) 
WHEN ticketTbl.ttd_trxn_date LIKE '%-%-%' AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) > 12 AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f'))-12 > 9 THEN CONCAT(TRY_CAST(EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f'))-12 AS VARCHAR),':00PM')
WHEN ticketTbl.ttd_trxn_date LIKE '%-%-%' AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) < 12 AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) < 10 THEN CONCAT('0',CONCAT(TRY_CAST(EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) AS VARCHAR),':00AM')) 
WHEN ticketTbl.ttd_trxn_date LIKE '%-%-%' AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) < 12 AND EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) > 9 THEN CONCAT(TRY_CAST(EXTRACT(HOUR FROM DATE_PARSE(ttd_trxn_date, '%Y-%m-%d %H:%i:%s.%f')) AS VARCHAR),':00AM')
ELSE null END AS "Hourly Sales by TRX",

ticket_type.tkt_type_seat_type AS "Seat Type",

CASE 
WHEN ticketTbl.ttd_pymt_platform = '' AND ticketTbl.ttd_pos_no = 'EPAY' THEN 'ONLINE'
WHEN ticketTbl.ttd_pymt_platform = '' AND ticketTbl.ttd_pos_no LIKE '%KIOSK%' THEN 'KIOSK' 
ELSE 'POS' 
END AS "Platform",

CASE 
WHEN ticketTbl.ttd_pymt_platform = '' AND ticketTbl.ttd_pos_no = 'EPAY' THEN 'ONLINE'
WHEN ticketTbl.ttd_pymt_platform = '' AND ticketTbl.ttd_pos_no LIKE '%KIOSK%' THEN 'KIOSK'
WHEN ticketTbl.ttd_pymt_platform = '' AND ticketTbl.ttd_pos_no = 'IOS' THEN 'MOBILE'
WHEN ticketTbl.ttd_pymt_platform = '' AND ticketTbl.ttd_pos_no = 'ANDROID' THEN 'MOBILE' 
ELSE 'POS' 
END AS "Source"

FROM "parquetS3"."ticketTbl"

LEFT JOIN "parquetS3"."trxn_refund_detail" ON TRY_CAST(ticketTbl.ttd_booking_id AS BIGINT) = TRY_CAST(TRY_CAST(trxn_refund_detail.trd_booking_id AS DOUBLE) AS BIGINT) AND ticketTbl.ttd_row_id = trxn_refund_detail.trd_row_id AND ticketTbl.ttd_col_id = trxn_refund_detail.trd_col_id

LEFT JOIN "parquetS3"."film" ON TRY_CAST(ticketTbl.ttd_film_id AS BIGINT) = film.film_id

LEFT JOIN "parquetS3"."film_parent_child" ON TRY_CAST(film.film_film_parent_id AS BIGINT) = film_parent_child.flm_parent_id

LEFT JOIN "parquetS3"."ticket_type" ON ticketTbl.ttd_tkt_type = ticket_type.tkt_type_id

RIGHT JOIN (
SELECT showid, TRY_CAST(locid AS BIGINT) AS "locid", showdate, totalseats FROM parquetS3.schedule_2018
UNION
SELECT showid, locid, showdate, totalseats FROM parquetS3.schedule_2019
UNION
SELECT showid, locid, showdate, totalseats FROM parquetS3.schedule_2020
)schedule ON TRY_CAST(ticketTbl.ttd_show_id AS BIGINT) = schedule.showid AND ticketTbl.ttd_loc_id = schedule.locid AND CASE WHEN ticketTbl.ttd_show_date LIKE '%-%-%' THEN DATE_PARSE(ticketTbl.ttd_show_date, '%Y-%m-%d') WHEN ticketTbl.ttd_show_date LIKE '%/%/%' THEN DATE_PARSE(ticketTbl.ttd_show_date, '%d/%m/%Y') ELSE null END = DATE_PARSE(schedule.showdate, '%Y-%m-%d %H:%i:%s.%f')

WHERE trxn_refund_detail.trd_booking_id is null AND ticketTbl.ttd_booking_id is not null AND schedule.showdate != ''

GROUP BY ticketTbl.ttd_loc_id, ticketTbl.ttd_bus_date, ticketTbl.ttd_row_id, ticketTbl.ttd_col_id, ticketTbl.ttd_pos_no, ticketTbl.ttd_film_id, ticketTbl.ttd_hall_id, ticketTbl.ttd_trxn_date, ticketTbl.ttd_ticket_no, ticketTbl.ttd_tkt_type, ticketTbl.ttd_pymt_channel_type, ticketTbl.ttd_show_time, ticketTbl.ttd_show_date, ticketTbl.ttd_pymt_platform, schedule.showdate, ticket_type.tkt_type_seat_type, ticketTbl.ttd_booking_id, ticketTbl.ttd_show_id;