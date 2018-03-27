package datasplash.bq;

import clojure.lang.IFn;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Objects;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import static com.google.common.base.Objects.equal;
import static org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions.checkNotNull;

@SuppressWarnings("NonSerializableFieldInSerializableClass")
public class ClojureDynamicDestinations extends DynamicDestinations<Object, Object> {

    private static final long serialVersionUID = 0L;
    private final IFn destinationFn;
    private final IFn tableFn;
    private final IFn schemaFn;

    public ClojureDynamicDestinations(final IFn destinationFn, final IFn tableFn, final IFn schemaFn) {
        this.destinationFn = checkNotNull(destinationFn);
        this.tableFn = checkNotNull(tableFn);
        this.schemaFn = checkNotNull(schemaFn);
    }

    @Override
    public Object getDestination(final ValueInSingleWindow<Object> element) {
        return destinationFn.invoke(element);
    }

    @Override
    public TableDestination getTable(final Object destination) {
        final Object td = tableFn.invoke(destination);
        return TableDestination.class.cast(td);
    }

    @Override
    public TableSchema getSchema(final Object destination) {
        final Object ts = schemaFn.invoke(destination);
        return TableSchema.class.cast(ts);
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) return true;
        if (obj == null || !getClass().equals(obj.getClass())) {
            return false;
        }
        final ClojureDynamicDestinations other = (ClojureDynamicDestinations) obj;
        return equal(destinationFn, other.destinationFn)
                && equal(tableFn, other.tableFn)
                && equal(schemaFn, other.schemaFn);
    }

    private int hash;

    @SuppressWarnings("NonFinalFieldReferencedInHashCode")
    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            //noinspection ObjectInstantiationInEqualsHashCode
            h = Objects.hashCode(destinationFn, tableFn, schemaFn);
            hash = h;
        }
        return h;
    }
}
