package com.datatorrent.template;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.KeyValPair;

public class HiLoOperator implements Operator {
	
	private HashMap<String, HighLow> ranges = new HashMap<String, HighLow>();
	
	public final transient DefaultInputPort<KeyValPair<String, Double>> input = new DefaultInputPort<KeyValPair<String, Double>>()
			  {
			    /**
			     * Reference counts tuples
			     */
			    @Override
			    public void process(KeyValPair<String, Double> t)
			    {
			      HighLow hi = ranges.get(t.getKey());
			      if (hi == null) {
			    	  hi = new HighLow(t.getValue(), t.getValue());
			    	  ranges.put(t.getKey(), hi);
			      } else {
			    	  if (t.getValue().doubleValue() < hi.getLow().doubleValue()) {
			    		  hi.setLow(t.getValue());
			    	  }
			    	  if (t.getValue().doubleValue() < hi.getHigh().doubleValue()) {
			    		  hi.setHigh(t.getValue());
			    	  }
			      }
			    }
			  };
	
			  @OutputPortFieldAnnotation(name = "ranges")
			  public final transient DefaultOutputPort<HashMap<String, HighLow>> output = new DefaultOutputPort<HashMap<String, HighLow>>();	

	@Override
	public void setup(OperatorContext arg0) {
	}

	@Override
	public void teardown() {
	}

	@Override
	public void beginWindow(long arg0) {
	}

	@Override
	public void endWindow() {
		this.output.emit(ranges);
		ranges = new HashMap<String, HighLow>();
	}

}
