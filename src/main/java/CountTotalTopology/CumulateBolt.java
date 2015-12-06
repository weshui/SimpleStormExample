package CountTotalTopology;

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
import java.util.Map;

/**
 * Created by Wesley on 12/5/15.
 */
public class CumulateBolt extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();
    String outPath;
    PrintWriter out;
    Long counter;

    public void prepare(Map stormConf,
                        TopologyContext context) {
        outPath = stormConf.get("outputFile").toString();
        counter = 0L;
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
        counter++;
        counts.put(word, count);
        collector.emit(new Values(word, count));

        out.print(counter + "\n");
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
