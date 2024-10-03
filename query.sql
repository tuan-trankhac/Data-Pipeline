show catalogs;
set CATALOG 'iceberg';
show databases;
use bot_db;
-- show tables;

-- PKG_CHAT_LOG.PGET_DU_LIEU_CHAT 
 select
        text_id,
        sender_text,
        bot_response,
        sender_id,
        created_time,
        sender_intent
  from bot_response;


-- PKG_CHAT_LOG.GET_LAST_MESS_BY_COUNT
-- Lấy danh sách người chat và câu chat cuối cùng của người đó 
WITH LatestMessages AS (
    SELECT
        USER_ID,
        SENDER_TEXT,
        ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY CREATED_TIME DESC) AS rn
    FROM bot_db.bot_response
    WHERE DATE(CREATED_TIME) <= CURRENT_DATE
),
FinalMessages AS (
    SELECT
        USER_ID,
        SENDER_TEXT
    FROM LatestMessages
    WHERE rn = 1
)
SELECT
    USER_ID,
    SENDER_TEXT,
    (SELECT COUNT(*) FROM FinalMessages) AS total_record
FROM FinalMessages;



/* 
Yêu cầu: Lấy câu chat của người dùng và phản hồi của bot trong một cuộc hội thoại
Yêu cầu bổ sung:
- Dữ liệu được lọc theo sender_id, bot_id
- Dữ liệu được lọc theo created_time, và sắp xếp theo created_time
*/
-- PKG_CHAT_LOG.PGET_CHAT_HIS_BY_ID
SELECT
    sender_id,
    bot_id,
    created_time,
    sender_text,
    bot_response,
    total_time,
    search_time,
    rewrite_time,
    retrieval_time,
    generation_time
FROM bot_response
WHERE sender_id = "217b05ed-52ef-49ca-89cb-401373fde0ef"
  AND bot_id = "99ce9950-1ca5-11ef-981d-ffb7de893de8"
  AND DATE(created_time) = DATE("2024-07-09")
ORDER BY created_time ASC;



-- Get chat history by sender_id
set @filter_sender_id = "217b05ed-52ef-49ca-89cb-401373fde0ef";
set @bot_id = "99ce9950-1ca5-11ef-981d-ffb7de893de8";
set @filter_time = DATE("2024-07-09");

select *
from PGET_CHAT_HIS_BY_ID
where sender_id = @filter_sender_id
AND bot_id = @bot_id
AND DATE(created_time) = DATE(@filter_time)
order by created_time asc;


/*
PKG_BOT_PREDICT.PGET_BOT_PREDICT_STEP
Yêu cầu: Thống kê các kịch bản (step_names)
Mỗi kịch bản có bao nhiêu tin nhắn, chiếm bao nhiêu phần trăm tổng số kịch bản?

Yêu cầu bổ sung:
Lọc theo một khoảng thời gian
*/
-- Đặt filter cho thời gian
SET @time_filter_1 = DATE('2024-05-05');
SET @time_filter_2 = DATE('2024-08-08');

-- Truy vấn trực tiếp, xử lý mảng step_names với tối đa 5 phần tử
SELECT 
    scenario,
    COUNT(*) AS scenario_count,
    total_steps.total_step,
    COUNT(*) / total_steps.total_step AS percentage
FROM (
    SELECT 
        created_time,
        SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 1), ',', -1) AS scenario
    FROM bot_response
    WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
    
    UNION ALL
    
    SELECT 
        created_time,
        SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 2), ',', -1) AS scenario
    FROM bot_response
    WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
    
    UNION ALL
    
    SELECT 
        created_time,
        SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 3), ',', -1) AS scenario
    FROM bot_response
    WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
    
    UNION ALL
    
    SELECT 
        created_time,
        SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 4), ',', -1) AS scenario
    FROM bot_response
    WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
    
    UNION ALL
    
    SELECT 
        created_time,
        SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 5), ',', -1) AS scenario
    FROM bot_response
    WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
) AS UnnestedSteps
JOIN (
    SELECT COUNT(*) AS total_step
    FROM (
        SELECT 
            created_time,
            SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 1), ',', -1) AS scenario
        FROM bot_response
        WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
        
        UNION ALL
        
        SELECT 
            created_time,
            SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 2), ',', -1) AS scenario
        FROM bot_response
        WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
        
        UNION ALL
        
        SELECT 
            created_time,
            SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 3), ',', -1) AS scenario
        FROM bot_response
        WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
        
        UNION ALL
        
        SELECT 
            created_time,
            SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 4), ',', -1) AS scenario
        FROM bot_response
        WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
        
        UNION ALL
        
        SELECT 
            created_time,
            SUBSTRING_INDEX(SUBSTRING_INDEX(step_names, ',', 5), ',', -1) AS scenario
        FROM bot_response
        WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
    ) AS AllSteps
) AS total_steps
GROUP BY scenario, total_steps.total_step;


-- Tính thời điểm hiện tại
WITH currenttime AS (
    SELECT CURRENT_TIMESTAMP() AS now
)
-- Tìm người dùng có tin nhắn trong vòng 30 phút qua
SELECT COUNT(DISTINCT SENDER_ID) AS active_users
FROM bot_dvc_hcm, currenttime
WHERE CREATED_TIME >= TIMESTAMPADD(MINUTE, -30, now);

WITH PGET_BOT_PREDICT_STEP AS (
    SELECT
        created_time,
        unnest
    FROM bot_response, UNNEST(step_names) AS unnest
    WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
),
-- CTE thứ hai để tính tổng số lượng kịch bản
TOTAL_STEP_CTE AS (
    SELECT COUNT(*) AS total_step
    FROM PGET_BOT_PREDICT_STEP
)

-- Tính toán số lượng kịch bản và tỷ lệ phần trăm
SELECT
    unnest, -- Tên kịch bản
    COUNT(*) AS scenario_count, -- Số lượng của mỗi kịch bản
    TOTAL_STEP_CTE.total_step, -- Tổng số kịch bản
    COUNT(*) / TOTAL_STEP_CTE.total_step AS percentage -- Tỷ lệ phần trăm của kịch bản
FROM PGET_BOT_PREDICT_STEP, TOTAL_STEP_CTE
GROUP BY unnest, TOTAL_STEP_CTE.total_step;

