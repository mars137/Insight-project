package com.atif.kafka.Message;

import avro.Message.Propensity;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import java.io.EOFException;
import java.io.IOException;

public class PropensityDeserializer {
    public PropensityDeserializer() {}

    public Propensity deserializeEvent(byte[] event) throws Exception {
        SpecificDatumReader<Propensity> reader = new SpecificDatumReader<Propensity>(Propensity.getClassSchema());
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
