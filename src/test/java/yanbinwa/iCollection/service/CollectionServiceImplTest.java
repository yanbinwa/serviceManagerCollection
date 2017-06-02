package yanbinwa.iCollection.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class CollectionServiceImplTest
{

    @Test
    public void test()
    {
        String kafkaTopicInfo = "{\"cacheTopic\":[\"cacheTopic_1\"]}";
        JSONObject topicInfo = new JSONObject(kafkaTopicInfo);
        Map<String, Set<String>> topicGroupToTopicMap = new HashMap<String, Set<String>>();
        for(Object topicGroupObj : topicInfo.keySet())
        {
            if(topicGroupObj == null)
            {
                continue;
            }
            if (!(topicGroupObj instanceof String))
            {
                continue;
            }
            String topicGroup = (String)topicGroupObj;
            JSONArray topicList = topicInfo.getJSONArray(topicGroup);
            Set<String> topicSet = topicGroupToTopicMap.get(topicGroup);
            if (topicSet == null)
            {
                topicSet = new HashSet<String>();
                topicGroupToTopicMap.put(topicGroup, topicSet);
            }
            for(int i = 0; i < topicList.length(); i ++)
            {
                String topic = topicList.getString(i);
                topicSet.add(topic);
            }
        }
        System.out.println(topicGroupToTopicMap);
    }

}
