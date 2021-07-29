package mm.functions;

import java.util.Collection;
import java.util.Vector;

import com.ibm.research.time_series.core.observation.Observation;
import com.ibm.research.time_series.core.timeseries.SegmentTimeSeries;
import com.ibm.research.time_series.core.timeseries.TimeSeries;
import com.ibm.research.time_series.core.utils.ObservationCollection;
import com.ibm.research.time_series.core.utils.Observations;
import com.ibm.research.time_series.core.utils.TSBuilder;
import com.ibm.research.time_series.transforms.reducers.math.MathReducers;
import com.ibm.research.time_series.transforms.transformers.math.MathTransformers;

import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.WindowContext;
import org.apache.pulsar.functions.api.WindowFunction;
import org.slf4j.Logger;

public class AnomalyScorer implements WindowFunction<Double, TimeSeries<Double>> {
    @Override
    public TimeSeries<Double> process(Collection<Record<Double>> inputs, WindowContext context) throws Exception {
        //int numInstances = context.getNumInstances();
        //System.out.println(numInstances);

        Double cumsum = 0.0;
        Vector<Double> retval = null;
        TSBuilder<Double> tsBuilder = Observations.newBuilder();

        int i = 0;
        for (Record<Double> record : inputs) {
            cumsum += record.getValue();
            retval.addElement(cumsum);
            tsBuilder.add(new Observation<>(i++,(double)record.getValue())); 
        }

        //Step 0: create TimeSeries'
        TimeSeries<Double> ts = TimeSeries.fromObservations(tsBuilder.result());

        //return String.join(",", integers);
        return ts;
    }
}
