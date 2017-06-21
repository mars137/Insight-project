package com.atif.kafka.Message;

        import avro.Message.Event;
        import org.apache.avro.file.DataFileWriter;
        import org.apache.avro.io.BinaryEncoder;
        import org.apache.avro.io.DatumWriter;
        import org.apache.avro.io.EncoderFactory;
        import org.apache.avro.io.JsonEncoder;
        import org.apache.avro.specific.SpecificDatumWriter;
        import org.apache.commons.io.output.ByteArrayOutputStream;

        import java.io.BufferedOutputStream;
        import java.io.IOException;
        import java.io.OutputStream;

public class EventSerializer {

    public EventSerializer() throws IOException {}

    public byte[] serializeMessage(Event eventMessage) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<Event> writer = new SpecificDatumWriter<Event>(Event.getClassSchema());
        writer.write(eventMessage, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    public String serializeMessageToJSON(Event eventMessage) throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(eventMessage.getSchema(), out, true);
        DatumWriter<Event> writer = new SpecificDatumWriter<Event>(Event.getClassSchema());
        writer.write(eventMessage, encoder);
        encoder.flush();
        out.close();
        return out.toString();
    }
}