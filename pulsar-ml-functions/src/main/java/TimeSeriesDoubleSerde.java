package mm.functions;

import java.util.Collection;
import java.util.Vector;
import java.util.Arrays;

import org.apache.pulsar.functions.api.SerDe;

import com.ibm.research.time_series.core.observation.Observation;
import com.ibm.research.time_series.core.timeseries.SegmentTimeSeries;
import com.ibm.research.time_series.core.timeseries.TimeSeries;
import com.ibm.research.time_series.core.utils.ObservationCollection;
import com.ibm.research.time_series.core.utils.Observations;
import com.ibm.research.time_series.core.utils.TSBuilder;
import com.ibm.research.time_series.transforms.reducers.math.MathReducers;
import com.ibm.research.time_series.transforms.transformers.math.MathTransformers;

import java.util.regex.Pattern;

public class TimeSeriesDoubleSerde implements SerDe<TimeSeries<Double>> {
    public TimeSeries<Double> deserialize(byte[] input) {
        TimeSeries<Double> ts = TimeSeries.list(Arrays.asList(1.0, 2.0, 3.0, null, 5.0, 6.0, null, 9.0));
        return ts;
    }

    public byte[] serialize(TimeSeries<Double> input) {
        //return "%s|%s".format(input.getUsername(), input.getTweetContent()).getBytes();
        return input.toString().getBytes();
    }
}
