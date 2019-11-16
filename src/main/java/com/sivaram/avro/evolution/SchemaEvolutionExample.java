package com.sivaram.avro.evolution;

import com.example.CustomerV1;
import com.example.CustomerV2;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class SchemaEvolutionExample {
    public static void main(String[] args) throws Exception {
        CustomerV1.Builder customerV1Builder = CustomerV1.newBuilder();
        customerV1Builder.setFirstName("John");
        customerV1Builder.setLastName("Doe");
        customerV1Builder.setAge(25);
        customerV1Builder.setHeight(150.5f);
        customerV1Builder.setWeight(85.5f);
        customerV1Builder.setAutomatedEmail(true);
        CustomerV1 customerV1 = customerV1Builder.build();
        System.out.println(customerV1.toString());
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        final DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(customerV1.getSchema(), new File("customerV1.avro"));
        dataFileWriter.append(customerV1);
        dataFileWriter.close();
        System.out.println("successfully wrote customerV1.avro");

        final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
        final DataFileReader<CustomerV2> dataFileReader;
        File file = new File("customerV1.avro");
      dataFileReader = new DataFileReader<CustomerV2>(file,datumReader);
      while (dataFileReader.hasNext()){
          CustomerV2 customerV2 = dataFileReader.next();
          System.out.println(customerV2.toString());
      }

        CustomerV2.Builder customerV2Builder = CustomerV2.newBuilder();
        customerV2Builder.setFirstName("John");
        customerV2Builder.setLastName("Doe");
        customerV2Builder.setAge(25);
        customerV2Builder.setHeight(150.5f);
        customerV2Builder.setWeight(85.5f);
        customerV2Builder.setEmail("test@test.com");
        customerV2Builder.setPhoneNumber("727000000");
        CustomerV2 customerV2 = customerV2Builder.build();
        System.out.println(customerV2.toString());
        final DatumWriter<CustomerV2> datumWriter2 = new SpecificDatumWriter<>(CustomerV2.class);
        final DataFileWriter<CustomerV2> dataFileWriter2 = new DataFileWriter<>(datumWriter2);
        dataFileWriter2.create(customerV2.getSchema(), new File("customerV2.avro"));
        dataFileWriter2.append(customerV2);
        dataFileWriter2.close();
        System.out.println("successfully wrote customerV2.avro");

        final DatumReader<CustomerV1> datumReader2 = new SpecificDatumReader<>(CustomerV1.class);
        final DataFileReader<CustomerV1> dataFileReader2;
        File file2 = new File("customerV2.avro");
        dataFileReader2 = new DataFileReader<CustomerV1>(file2,datumReader2);
        while (dataFileReader2.hasNext()){
            CustomerV1 customerV1_2 = dataFileReader2.next();
            System.out.println(customerV1_2.toString());
        }
    }
}
