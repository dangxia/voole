SELECT 
  FIRST_VALUE (DIM_OEM_ID) WITHIN
GROUP (
ORDER BY METRIC_PLAYBGNTIME DESC) AS oemid,
FIRST_VALUE (
  CASE
    WHEN METRIC_AVGSPEED IS NULL 
    OR BITRATE IS NULL 
    OR BITRATE * 1024 <= METRIC_AVGSPEED * 8 
    THEN 0 
    ELSE 1 
  END
) WITHIN
GROUP (
ORDER BY METRIC_PLAYBGNTIME DESC) AS is_low 
FROM
  HIVEORDERDETAILRECORD_PHOENIX
WHERE METRIC_PLAYBGNTIME IS NOT NULL 
  AND METRIC_PLAYBGNTIME > CAST(CURRENT_DATE() AS BIGINT) / 1000 - 10800 
  AND 
  CASE
    WHEN METRIC_PLAYENDTIME IS NOT NULL 
    AND METRIC_PLAYENDTIME > METRIC_PLAYBGNTIME 
    THEN - 1 
    ELSE 
    CASE
      WHEN METRIC_PLAYALIVETIME IS NOT NULL 
      AND METRIC_PLAYALIVETIME > METRIC_PLAYBGNTIME 
      THEN METRIC_PLAYALIVETIME 
      ELSE METRIC_PLAYBGNTIME 
    END 
  END >  CAST(CURRENT_DATE() AS BIGINT) / 1000 - 600 
GROUP BY DIM_USER_HID 


UPSERT INTO fact_vod_history (
  DAY, sessid, stamp, userip, datasorce, playurl, VERSION, dim_date_hour, dim_isp_id, dim_user_uid, dim_user_hid, dim_oem_id, dim_area_id, dim_area_parentid, dim_nettype_id, dim_media_fid, dim_media_series, dim_media_mimeid, dim_movie_mid, dim_cp_id, dim_movie_category, dim_product_pid, dim_product_ptype, dim_po_id, dim_epg_id, dim_section_id, dim_section_parentid, metric_playbgntime, metric_playalivetime, metric_playendtime, metric_durationtime, metric_avgspeed, metric_isad, metric_isrepeatmod, metric_status, metric_techtype, metric_partnerinfo, extinfo, vssip, perfip, bitrate
) 
SELECT 
  TO_CHAR (
    CAST(
      METRIC_PLAYBGNTIME * 1000 AS TIMESTAMP
    ), 'yyyy-MM-dd'
  ) AS DATE, sessid, stamp, userip, datasorce, playurl, VERSION, dim_date_hour, dim_isp_id, dim_user_uid, dim_user_hid, dim_oem_id, dim_area_id, dim_area_parentid, dim_nettype_id, dim_media_fid, dim_media_series, dim_media_mimeid, dim_movie_mid, dim_cp_id, dim_movie_category, dim_product_pid, dim_product_ptype, dim_po_id, dim_epg_id, dim_section_id, dim_section_parentid, metric_playbgntime, metric_playalivetime, metric_playendtime, metric_durationtime, metric_avgspeed, metric_isad, metric_isrepeatmod, metric_status, metric_techtype, metric_partnerinfo, extinfo, vssip, perfip, bitrate 
FROM
  HIVEORDERDETAILRECORD_PHOENIX 
WHERE METRIC_PLAYBGNTIME IS NOT NULL 
  AND METRIC_PLAYBGNTIME <= CAST(CURRENT_DATE() AS BIGINT) / 1000 - 10800 