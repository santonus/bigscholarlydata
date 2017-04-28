# Saves topic keywords of topics in more readable format in a txt file
# Keywords are arranged in decreasing order of prob
mysql -u bigdataproj -pbigdataproj@123 -h n12 data_AM  -e "select topic_id, GROUP_CONCAT(keyword ORDER BY prob desc) from  TopicKeywords_se_60 GROUP BY topic_id order by topic_id,prob;
" > TopicsKeywords_se_60.txt
mysql -u bigdataproj -pbigdataproj@123 -h n12 data_AM  -e "select topic_id, GROUP_CONCAT(keyword ORDER BY prob desc) from  TopicKeywords_db_60 GROUP BY topic_id order by topic_id,prob;
" > TopicsKeywords_db_60.txt
mysql -u bigdataproj -pbigdataproj@123 -h n12 data_AM  -e "select topic_id, GROUP_CONCAT(keyword ORDER BY prob desc) from  TopicKeywords_ai_60 GROUP BY topic_id order by topic_id,prob;
" > TopicsKeywords_ai_60.txt
mysql -u bigdataproj -pbigdataproj@123 -h n12 data_AM  -e "select topic_id, GROUP_CONCAT(keyword ORDER BY prob desc) from  TopicKeywords_os_60 GROUP BY topic_id order by topic_id,prob;
" > TopicsKeywords_os_60.txt