###########################
##### Procedure List #####
###########################

CREATE PROCEDURE query_tweets()
BEGIN
    SELECT tweet_id from tweets;
END;


##########################

CREATE PROCEDURE save_new_video_id(
    IN s_tweet_id INT,
    IN s_text TEXT,
    IN s_time DATETIME
)
BEGIN
    INSERT INTO tweets (tweet_id, text, created_at)
    VALUES (s_tweet_id, s_text, s_time);
end;