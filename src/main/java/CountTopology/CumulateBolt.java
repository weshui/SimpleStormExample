package CountTopology;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Wesley on 12/5/15.
 */
public class CumulateBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    String key = new String();

    public void prepare(Map stormConf,
                        TopologyContext context) {
        key = stormConf.get("key").toString();
        System.out.println(key);
    }


    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count));
//        if(word.equals(key)){
            System.out.println("Currnet count: " + counts.get(key));
//        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
