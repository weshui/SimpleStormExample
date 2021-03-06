package CountTopology;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Wesley on 12/5/15.
 */
public class CumulateBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    String key;
    String outPath;
    PrintWriter out;

    public void prepare(Map stormConf,
                        TopologyContext context) {
        key = stormConf.get("key").toString().trim();
        outPath = stormConf.get("outputFile").toString();
        try {
            out = new PrintWriter(new BufferedWriter(new FileWriter(outPath, true)));
        } catch(Exception e){

        }
    }


    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String word = tuple.getString(0);
        Integer count = counts.get(word);
        if (count == null)
            count = 0;
        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count));
        if (word.trim().equals(key)) {
            System.out.println("Currnet count: " + counts.get(key));
            out.print(counts.get(key) + "\n");
        }
//        out.print(word);
//        out.print(key + " " + word);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

    public void cleanup(){
        out.print(" storm");
        Iterator it = counts.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            out.print(counts.get(pair.getKey()) + "\n");
            it.remove(); // avoids a ConcurrentModificationException
        }
    }
}
