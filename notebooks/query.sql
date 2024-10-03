/* PKG_CHAT_LOG.PGET_DU_LIEU_CHAT 
Yêu cầu: Lấy dữ liệu chat của bot. 
Gồm: 
- Câu chat của người dùng
- sender id
- Thời gian
- Ý định mà bot nhận diện
Yêu cầu bổ sung:
- Lọc theo thời gian
*/
CREATE OR REPLACE VIEW PGET_DU_LIEU_CHAT AS (
    SELECT
        text_id,
        sender_text,
        bot_response,
        sender_id,
        created_time,
        sender_intent
    FROM bot_response
);

SET @time_filter_1 = DATE('2024-05-05');
SET @time_filter_2 = DATE('2024-08-08');

SELECT * 
FROM PGET_DU_LIEU_CHAT
WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
ORDER BY created_time DESC;


/* PKG_CHAT_LOG.GET_LAST_MESS_BY_COUNT
Yêu cầu: Lấy danh sách người chat và câu chat cuối cùng của người đó 
Yêu cầu bổ sung:
- Lọc theo thời gian, mặc định: Hôm nay
*/
CREATE OR REPLACE VIEW LatestChatTime AS (
    SELECT user_id, MAX(created_time) AS latest_time
    FROM bot_response
    GROUP BY user_id
);

CREATE OR REPLACE VIEW GET_LAST_MESS_BY_COUNT AS (
    SELECT b.user_id, b.sender_text, b.created_time
    FROM bot_response b
    JOIN LatestChatTime lct ON b.user_id = lct.user_id AND b.created_time = lct.latest_time
    order by created_time desc
);

select * from GET_LAST_MESS_BY_COUNT;
-- filter theo time: mặc định hôm nay:
set @filter_time = DATE("2024-07-15");
select @filter_time;

SELECT *
FROM GET_LAST_MESS_BY_COUNT
WHERE DATE(created_time) = DATE(@filter_time)

-- PKG_CHAT_LOG.PGET_CHAT_HIS_BY_ID
/* 
Yêu cầu: Lấy câu chat của người dùng và phản hồi của bot trong một cuộc hội thoại
Yêu cầu bổ sung:
- Dữ liệu được lọc theo sender_id, bot_id
- Dữ liệu được lọc theo created_time, và sắp xếp theo created_time
*/
CREATE OR REPLACE VIEW PGET_CHAT_HIS_BY_ID AS (
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
);


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
set @time_filter_1 = DATE('2024-05-05');
set @time_filter_2 = DATE('2024-08-08');

-- Tạo view PGET_BOT_PREDICT_STEP để unnest step_names. Nguyên nhân là bì step_name là Array<String>
CREATE OR REPLACE VIEW PGET_BOT_PREDICT_STEP AS (
    SELECT
        step_names,
        UNNEST(step_names) AS unnest_step_name,
        created_time
    FROM bot_response
);

-- CTE để lưu tổng số lượng kịch bản
WITH TOTAL_STEP_CTE AS (
    SELECT COUNT(*) AS total_step
    FROM PGET_BOT_PREDICT_STEP
    WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2
)

-- Tính toán số lượng kịch bản và tỷ lệ phần trăm
SELECT
    unnest_step_name AS scenario, -- Tên kịch bản
    COUNT(*) AS scenario_count, -- Số lượng của mỗi kịch bản
    TOTAL_STEP_CTE.total_step, -- Tổng số kịch bản
    COUNT(*) / TOTAL_STEP_CTE.total_step AS percentage -- Tỷ lệ phần trăm của kịch bản
FROM PGET_BOT_PREDICT_STEP, TOTAL_STEP_CTE
WHERE created_time BETWEEN @time_filter_1 AND @time_filter_2 -- Lọc trong khoảng thời gian
GROUP BY unnest_step_name, TOTAL_STEP_CTE.total_step;


-- PKG_BOT_PREDICT.PREPORT_BOT_PREDICT (ok)
	-- Thống kê số lượng tin nhắn bot hiểu (không hiểu) 
	-- Giả  Thresholder = 0.7
SELECT
    CASE 
        WHEN INTENT_CONFIDENCE >= 0.7 THEN 'Understood'
        ELSE 'Not Understood'
    END AS understanding,
    COUNT(*) AS message_count
FROM
    bot_ddvc_hcm_bot_predict
GROUP BY
    CASE 
        WHEN INTENT_CONFIDENCE >= 0.7 THEN 'Understood'
        ELSE 'Not Understood'
    END;


-- PKG_BOT_PREDICT.PREPORT_NGHIEP_VU_BOT_PREDICT 
	--   Thống kê ý định 
SELECT 
    INTENT_NAME,
    COUNT(*) AS intent_count
FROM 
    bot_ddvc_hcm_bot_predict
GROUP BY 
    INTENT_NAME
ORDER BY 
    intent_count DESC;

-- PKG_USER.PREPORT_USER
	-- Update: (Thêm bảng thứ nguyên) . Đếm số lượng bản ghi tới thời điểm hiện tại trước khi update. 
				-- 1 bảng thứ nguyên theo created_time 1 bảng theo latest_time
	-- => Thống kê số lượng người chat
	 
-- Đếm số lượng bản ghi

SELECT COUNT(*) AS total_records
FROM bot_dvc_hcm;

-- Tạo bảng thứ nguyên theo created_time

CREATE TABLE bot_dvc_hcm_created_time AS
SELECT
    DATE(created_time) AS day,
    COUNT(*) AS count_records
FROM bot_dvc_hcm
GROUP BY DATE(created_time);

-- Thống kê số lượng người chat
SELECT COUNT(DISTINCT SENDER_ID) AS unique_senders
FROM bot_dvc_hcm;

-- PKG_USER.PREPORT_NEW_ACTIVE_USER
	-- Tính thời điểm hiện tại
WITH current_time AS (
    SELECT CURRENT_TIMESTAMP() AS now
)
	-- Giả sử người dùng có tin nhắn trong vòng 30 phút là đang hoạt 
SELECT COUNT(DISTINCT SENDER_ID) AS active_users
FROM bot_dvc_hcm, current_time
WHERE CREATED_TIME >= TIMESTAMPADD(MINUTE, -30, now);