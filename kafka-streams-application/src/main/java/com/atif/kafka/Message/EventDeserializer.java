package com.atif.kafka.Message;

        import avro.Message.Event;
        import org.apache.avro.io.Decoder;
        import org.apache.avro.io.DecoderFactory;
        import org.apache.avro.specific.SpecificDatumReader;
        import java.io.EOFException;
        import java.io.IOException;

public class EventDeserializer {
    public EventDeserializer() {}

    public Event deserializeEvent(byte[] event) throws Exception {
        SpecificDatumReader<Event> reader = new SpecificDatumReader<Event>(Event.getClassSchema());
        Decoder decoder = null;
        try{
            decoder = DecoderFactory.get().binaryDecoder(event, null);
            return reader.read(null, decoder);
        } catch(EOFException exception){
            System.out.println("here0");
            exception.printStackTrace();
        } catch(IOException exception){
            System.out.println("here1");
            exception.printStackTrace();
        }
        return null;
    }
}
