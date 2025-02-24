select user_id, user_first_name, context_date, count(1) as message_amount from telegram

group by user_id, user_first_name, context_date order by context_date desc
 