package com.kaseya.trident;

import java.util.List;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.spout.IBatchSpout;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.spout.ITridentSpout;
import backtype.storm.tuple.Fields;

import com.kaseya.trident.operations.IStreamOperation;

public class StreamWrapper {
    protected Object _stream;

    /**
     * Adds operations onto passed in stream.
     * 
     * @param stream
     *            Passed in stream.
     * @param spoutNodeName
     *            Spout Name.
     * @param topology
     *            Trident topology.
     * @param operations
     *            Stream operations.
     * 
     * @see IStreamOperation
     */
    public StreamWrapper(final Stream stream, final TridentTopology topology,
                         final List<IStreamOperation> operations) {

        _stream = decorateStream_(stream, operations);
    }

    /**
     * Creates a stream from a join of streams.<br>
     * <b>Note -</b> Join would be an inner join.
     * 
     * @param streams
     *            List of streams to join.
     * @param joinFields
     *            Fields to join streams on.
     * @param outFields
     *            Fields in output stream.
     * @param spoutNodeName
     *            Spout name.
     * @param topology
     *            Trident topology.
     * @param operations
     *            Stream operations.
     * 
     * @see IStreamOperation
     */
    public StreamWrapper(final List<Stream> streams,
                         final List<Fields> joinFields, final Fields outFields,
                         final TridentTopology topology,
                         final List<IStreamOperation> operations) {

        _stream = decorateStream_(
                                  topology.join(streams, joinFields, outFields),
                                  operations);
    }

    /**
     * Creates a stream from a merge of streams.
     * 
     * @param outputFields
     *            - Fields in output stream.
     * @param streams
     *            - List of streams to merge.
     * @param spoutNodeName
     *            - Spout name.
     * @param topology
     * @param operations
     */
    public StreamWrapper(final Fields outputFields, final List<Stream> streams,
                         final TridentTopology topology,
                         final List<IStreamOperation> operations) {

        _stream = decorateStream_(topology.merge(outputFields, streams),
                                  operations);
    }

    public StreamWrapper(final IBatchSpout spout, final String spoutNodeName,
                         final TridentTopology topology,
                         final List<IStreamOperation> operations) {
        _stream = decorateStream_(topology.newStream(spoutNodeName, spout),
                                  operations);
    }

    public StreamWrapper(final ITridentSpout<?> spout,
                         final String spoutNodeName,
                         final TridentTopology topology,
                         final List<IStreamOperation> operations) {
        _stream = decorateStream_(topology.newStream(spoutNodeName, spout),
                                  operations);
    }

    public StreamWrapper(final IPartitionedTridentSpout<?, ?, ?> spout,
                         final String spoutNodeName,
                         final TridentTopology topology,
                         final List<IStreamOperation> operations) {
        _stream = decorateStream_(topology.newStream(spoutNodeName, spout),
                                  operations);
    }

    public StreamWrapper(final IOpaquePartitionedTridentSpout<?, ?, ?> spout,
                         final String spoutNodeName,
                         final TridentTopology topology,
                         final List<IStreamOperation> operations) {
        _stream = decorateStream_(topology.newStream(spoutNodeName, spout),
                                  operations);
    }

    public Stream getStream() {
        return castAs(Stream.class, _stream);
    }

    public TridentState getState() {
        return castAs(TridentState.class, _stream);
    }

    public GroupedStream getGroupedStream() {
        return castAs(GroupedStream.class, _stream);
    }

    protected static <T> T castAs(Class<T> type, Object obj) {
        return type.isInstance(obj) ? type.cast(obj) : null;
    }

    protected Object decorateStream_(final Object stream,
            final List<IStreamOperation> operations) {
        if (stream == null) {
            throw new RuntimeException(
                                       "Trying to build a Stream without a spout. StreamWrapper needs to be set with a spout.");
        }

        Object streamObj = stream;

        if (operations != null) {
            for (IStreamOperation op : operations) {
                streamObj = op.addStreamProcessor(streamObj);
            }
        }

        return streamObj;
    }
}
