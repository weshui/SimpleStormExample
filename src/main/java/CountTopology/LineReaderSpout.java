package CountTopology;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by Wesley on 12/5/15.
 */
public class LineReaderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String fileName;
    private BufferedReader reader;

    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        try {
            fileName = (String) conf.get("inputFile");
            reader = new BufferedReader(new FileReader(fileName));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file "
                    + conf.get("inputFile"));
        }
        this.collector = collector;
    }

    public void nextTuple() {
        try {
            String line = reader.readLine();
            if (line != null) {
                this.collector.emit(new Values(line));
            } else {
                System.out.println("Finished reading file!");
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void deactivate() {
        try {
            reader.close();
        } catch (IOException e) {

        }
    }

    @Override
    public void fail(Object msgId) {
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}
