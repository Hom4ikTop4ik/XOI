package ru.nsu.martynov;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Marshaller;
import org.xml.sax.SAXException;
import ru.nsu.martynov.model.People;
import ru.nsu.martynov.parser.DataProcessor;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class Main {
    public static void main(String[] args) {
        File inputFile = new File("people.xml");
        File outputFile = new File("output.xml");
        File xsdFile = new File("people.xsd");

        if (!inputFile.exists()) {
            System.err.println("File " + inputFile.getAbsolutePath() + " is not exists!");
            return;
        }

        try (InputStream is = new FileInputStream(inputFile)) {
            System.out.println("Task X1. Start DataProc: parse and merge 'people.xml'...");
            DataProcessor processor = new DataProcessor();
            People finalData = processor.processData(is);

            Thread.sleep(500);

            System.out.println("Merged. Unique people: " + finalData.getPersons().size());

            System.out.println("\n\n\nTask X2. Generate new 'output.xml' and XSD valid...");
            JAXBContext context = JAXBContext.newInstance(People.class);
            Marshaller marshaller = context.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            // Подключение строгой схемы XSD для валидации на этапе генерации
            SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(xsdFile);
            marshaller.setSchema(schema);

            marshaller.marshal(finalData, outputFile);
            System.out.println("Done! Check file '" + outputFile.getAbsolutePath() + "'.");

        } catch (SAXException e) {
            System.err.println("Validation XSD error (output.XML is bad): " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
