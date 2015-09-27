
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class FileReaderSpout implements IRichSpout {
  private SpoutOutputCollector _collector;
  private TopologyContext context;
  private BufferedReader _fileReader;
  
  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

	try {
		Object inputFileName = conf.get("inputFileName");
		if (inputFileName != null)
			_fileReader = new BufferedReader(new FileReader(inputFileName.toString()));
	} catch (Exception ex) {
		System.err.println("Error opening file: " + ex);
	}

    this.context = context;
    this._collector = collector;
  }

  @Override
  public void nextTuple() {

	if (_fileReader == null) {
		Utils.sleep(1000);
	} else {
		try {
			String line = _fileReader.readLine();
			if(line == null) {
				try {
					Utils.sleep(1000);
				} catch (Exception ignore) {}
			} else {
				_collector.emit(new Values(line));
			}
		} catch (Exception ex) {
			System.err.println("Error reading line: " + ex);
		}
	}
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    declarer.declare(new Fields("word"));

  }

  @Override
  public void close() {
	  
	  if (_fileReader != null) {
		try {
			_fileReader.close();
		} catch (IOException ignore) {
		}
	  }
  }


  @Override
  public void activate() {
  }

  @Override
  public void deactivate() {
  }

  @Override
  public void ack(Object msgId) {
  }

  @Override
  public void fail(Object msgId) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
