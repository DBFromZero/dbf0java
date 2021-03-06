package dbf0.disk_key_value.readwrite.lsmtree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import dbf0.common.ByteArrayWrapper;
import dbf0.common.io.ByteArraySerializer;
import dbf0.common.io.SerializationPair;
import dbf0.disk_key_value.readwrite.lsmtree.multivalue.ValueWrapper;
import dbf0.document.types.DElement;
import dbf0.document.types.DString;

import java.time.Duration;
import java.util.Comparator;

public class LsmTreeConfiguration<K, V> {

  public static final ByteArrayWrapper BYTE_ARRAY_DELETE_VALUE = ByteArrayWrapper.of(
      83, 76, 69, 7, 95, 21, 81, 27, 2, 104, 8, 100, 45, 109, 110, 1);
  public static final DString D_ELEMENT_DELETE_VALUE = new DString("|fxcR/*rwEC\\rMg/^");

  private final SerializationPair<K> keySerialization;
  private final SerializationPair<V> valueSerialization;
  private final Comparator<K> keyComparator;
  private final V deleteValue;
  private final Duration mergeCronFrequency;
  private final int pendingWritesDeltaThreshold;
  private final int indexRate;
  private final int maxInFlightWriteJobs;
  private final double maxDeltaReadPercentage;
  private final int maxDeltasPerMerge;

  private LsmTreeConfiguration(Builder<K, V> builder) {
    this.keySerialization = Preconditions.checkNotNull(builder.keySerialization);
    this.valueSerialization = Preconditions.checkNotNull(builder.valueSerialization);
    this.keyComparator = Preconditions.checkNotNull(builder.keyComparator);
    this.deleteValue = builder.deleteValue;
    this.mergeCronFrequency = Preconditions.checkNotNull(builder.mergeCronFrequency);
    this.pendingWritesDeltaThreshold = builder.pendingWritesDeltaThreshold;
    this.indexRate = builder.indexRate;
    this.maxInFlightWriteJobs = builder.maxInFlightWriteJobs;
    this.maxDeltaReadPercentage = builder.maxDeltaReadPercentage;
    this.maxDeltasPerMerge = builder.maxDeltasPerMerge;
  }

  public SerializationPair<K> getKeySerialization() {
    return keySerialization;
  }

  public SerializationPair<V> getValueSerialization() {
    return valueSerialization;
  }

  public Comparator<K> getKeyComparator() {
    return keyComparator;
  }

  public V getDeleteValue() {
    return deleteValue;
  }

  public Duration getMergeCronFrequency() {
    return mergeCronFrequency;
  }

  public int getPendingWritesDeltaThreshold() {
    return pendingWritesDeltaThreshold;
  }

  public int getIndexRate() {
    return indexRate;
  }

  public int getMaxInFlightWriteJobs() {
    return maxInFlightWriteJobs;
  }

  public double getMaxDeltaReadPercentage() {
    return maxDeltaReadPercentage;
  }

  public int getMaxDeltasPerMerge() {
    return maxDeltasPerMerge;
  }

  public Builder<K, V> toBuilder() {
    var b = new Builder<K, V>();
    b.pendingWritesDeltaThreshold = pendingWritesDeltaThreshold;
    b.keySerialization = keySerialization;
    b.valueSerialization = valueSerialization;
    b.keyComparator = keyComparator;
    b.deleteValue = deleteValue;
    b.mergeCronFrequency = mergeCronFrequency;
    b.indexRate = indexRate;
    b.maxInFlightWriteJobs = maxInFlightWriteJobs;
    b.maxDeltaReadPercentage = maxDeltaReadPercentage;
    return b;
  }

  @Override public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("keySerialization", keySerialization)
        .add("valueSerialization", valueSerialization)
        .add("keyComparator", keyComparator)
        .add("deleteValue", deleteValue)
        .add("mergeCronFrequency", mergeCronFrequency)
        .add("pendingWritesDeltaThreshold", pendingWritesDeltaThreshold)
        .add("indexRate", indexRate)
        .add("maxInFlightWriteJobs", maxInFlightWriteJobs)
        .add("maxDeltaReadPercentage", maxDeltaReadPercentage)
        .add("maxDeltasPerMerge", maxDeltasPerMerge)
        .toString();
  }

  public static class Builder<K, V> {
    private int pendingWritesDeltaThreshold = 10 * 1000;
    private SerializationPair<K> keySerialization;
    private SerializationPair<V> valueSerialization;
    private Comparator<K> keyComparator;
    private V deleteValue;
    private Duration mergeCronFrequency = Duration.ofSeconds(1);
    private int indexRate = 10;
    private int maxInFlightWriteJobs = 10;
    private double maxDeltaReadPercentage = 0.5;
    private int maxDeltasPerMerge = 20;

    public Builder<K, V> withPendingWritesDeltaThreshold(int pendingWritesDeltaThreshold) {
      Preconditions.checkArgument(pendingWritesDeltaThreshold > 0);
      this.pendingWritesDeltaThreshold = pendingWritesDeltaThreshold;
      return this;
    }

    public Builder<K, V> withKeySerialization(SerializationPair<K> keySerialization) {
      this.keySerialization = Preconditions.checkNotNull(keySerialization);
      return this;
    }

    public Builder<K, V> withValueSerialization(SerializationPair<V> valueSerialization) {
      this.valueSerialization = Preconditions.checkNotNull(valueSerialization);
      return this;
    }

    public Builder<K, V> withKeyComparator(Comparator<K> keyComparator) {
      this.keyComparator = Preconditions.checkNotNull(keyComparator);
      return this;
    }

    public Builder<K, V> withDeleteValue(V deleteValue) {
      this.deleteValue = Preconditions.checkNotNull(deleteValue);
      return this;
    }

    public Builder<K, V> withIndexRate(int indexRate) {
      Preconditions.checkArgument(indexRate > 0);
      this.indexRate = indexRate;
      return this;
    }

    public Builder<K, V> withMaxInFlightWriteJobs(int maxInFlightWriteJobs) {
      Preconditions.checkArgument(maxInFlightWriteJobs > 0);
      this.maxInFlightWriteJobs = maxInFlightWriteJobs;
      return this;
    }

    public Builder<K, V> withMaxDeltaReadPercentage(double maxDeltaReadPercentage) {
      Preconditions.checkArgument(maxDeltaReadPercentage > 0);
      Preconditions.checkArgument(maxDeltaReadPercentage < 1);
      this.maxDeltaReadPercentage = maxDeltaReadPercentage;
      return this;
    }

    public Builder<K, V> withMaxDeltasPerMerge(int maxDeltasPerMerge) {
      Preconditions.checkArgument(maxDeltasPerMerge > 0);
      this.maxDeltasPerMerge = maxDeltasPerMerge;
      return this;
    }

    public Builder<K, V> withMergeCronFrequency(Duration mergeCronFrequency) {
      Preconditions.checkArgument(!mergeCronFrequency.isZero());
      Preconditions.checkArgument(!mergeCronFrequency.isNegative());
      this.mergeCronFrequency = mergeCronFrequency;
      return this;
    }

    public LsmTreeConfiguration<K, V> build() {
      return new LsmTreeConfiguration<>(this);
    }
  }

  public static <K, V> Builder<K, V> builder() {
    return new Builder<>();
  }

  public static Builder<ByteArrayWrapper, ByteArrayWrapper> builderForBytes() {
    return LsmTreeConfiguration.<ByteArrayWrapper, ByteArrayWrapper>builder()
        .withKeySerialization(ByteArraySerializer.serializationPair())
        .withValueSerialization(ByteArraySerializer.serializationPair())
        .withKeyComparator(ByteArrayWrapper::compareTo)
        .withDeleteValue(BYTE_ARRAY_DELETE_VALUE);
  }

  public static Builder<DElement, DElement> builderForDocuments() {
    return LsmTreeConfiguration.<DElement, DElement>builder()
        .withKeySerialization(DElement.serializationPair())
        .withValueSerialization(DElement.sizePrefixedSerializationPair())
        .withKeyComparator(DElement::compareTo)
        .withDeleteValue(D_ELEMENT_DELETE_VALUE);
  }

  public static Builder<ByteArrayWrapper, ValueWrapper<ByteArrayWrapper>> builderForMultiValueBytes() {
    return LsmTreeConfiguration.<ByteArrayWrapper, ValueWrapper<ByteArrayWrapper>>builder()
        .withKeySerialization(ByteArraySerializer.serializationPair())
        .withValueSerialization(ValueWrapper.serializationPair(ByteArraySerializer.serializationPair()))
        .withKeyComparator(ByteArrayWrapper::compareTo);
  }

  public static Builder<DElement, ValueWrapper<DElement>> builderForMultiValueDocuments() {
    return LsmTreeConfiguration.<DElement, ValueWrapper<DElement>>builder()
        .withKeySerialization(DElement.serializationPair())
        .withValueSerialization(ValueWrapper.serializationPair(DElement.sizePrefixedSerializationPair()))
        .withKeyComparator(DElement::compareTo);
  }

  public static Builder<DElement, ValueWrapper<DElement>> builderForMultiValueDocumentsIndex() {
    return LsmTreeConfiguration.<DElement, ValueWrapper<DElement>>builder()
        .withKeySerialization(DElement.serializationPair())
        .withValueSerialization(ValueWrapper.serializationPair(DElement.serializationPair()))
        .withKeyComparator(DElement::compareTo);
  }
}
