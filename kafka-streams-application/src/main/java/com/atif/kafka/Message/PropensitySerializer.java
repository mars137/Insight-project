package com.atif.kafka.Message;

import avro.Message.Propensity;
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

public class PropensitySerializer {

    public PropensitySerializer() throws IOException {}

    public byte[] serializeMessage(Propensity propensityMessage) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        DatumWriter<Propensity> writer = new SpecificDatumWriter<Propensity>(Propensity.getClassSchema());
        writer.write(propensityMessage, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    public String serializeMessageToJSON(Propensity eventMessage) throws IOException {
        OutputStream out = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(eventMessage.getSchema(), out, true);
        DatumWriter<Propensity> writer = new SpecificDatumWriter<Propensity>(Propensity.getClassSchema());
        writer.write(eventMessage, encoder);
        encoder.flush();
        out.close();
        return out.toString();
    }
}