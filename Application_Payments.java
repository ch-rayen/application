package application;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import system.Formatter;
import system.IoC;

import system.DataFactory;
import system.IoC;


/**
 * Executable application class with main() function.
 * 
 * @version <code style=color:green>{@value application.package_info#Version}</code>
 * @author <code style=color:blue>{@value application.package_info#Author}</code>
 */
public class Application_Payments {
    private final ObjectMapper mapper = new ObjectMapper();
    private final DateTimeFormatter dtf_json = DateTimeFormatter.ISO_DATE_TIME;
    private final DateTimeFormatter dtf_print = DateTimeFormatter.ofPattern("dd.MMM yyyy, HH:mm:ss (+SSS'ms')");
    //
    private final long lower = toLong(toLocalDateTime(dtf_json, "2020-01-01T00:00:00.000").get());
    private final long upper = toLong(LocalDateTime.now());

    /**
     * Private constructor to prevent external instance creation.
     */
    private Application_Payments() { }


    /**
     * Public main() function.
     * 
     * @param args arguments passed from command line.
     */
    public static void main(String[] args) {
        var appInstance = new Application_Payments();
        appInstance.run();
    }

 
    /**
     * Run function invoked in main after Application instance has been created.
     * 
     */
    private void run() {
        String msg = String.format("Hello, %s!", "payment system");
        System.out.println(msg);
        //
        // read and create objects from JSON file
        // createfromJSON("./data/payments.json", "payments",
        DataFactory factory = IoC.getInstance().getDataFactory();
        factory.createPaymentsFromJSON("./data/payments.json", "payments",
            // creator function that attempts to create object from JsonNode properties
            jn -> factory.createPayment(jn)         // returns Optional
        ).stream()
            .map(payment -> {
                String received = payment.getReceived()
                .format(DateTimeFormatter.ofPattern("dd.MMM yyyy, HH:mm"));
                long amountL = payment.getAmount();
                String amountStr = String.format("%4d.%02d EUR", amountL/100L, amountL%100L);
                //
                String formatted = String.format(
                    "Zahlungseingang am [%s Uhr] fuer Bestellung (%s) ueber %s", 
                    received, payment.getOrder_id(), amountStr);
                //
                return formatted;
            })
            .forEach(System.out::println);  // print objects
        long total = createfromJSON("./data/payments.json", "payments", jn -> createPayment(jn))
            .stream()
            // Stream of payments as String:
            // "payment(PY-529999371) for order(8592356245) of [ 129.79 EUR], received 26.Nov..."
            // .peek(System.out::println)  // print payments as String
            //
            // Vervollständigen Sie den Stream, so dass der Wert: "[ 129.79 EUR]" aus dem String 
            // ausgeschnitten und in einen long-Wert umgewandelt wird, der am Ende summiert 
            // werden kann mit:
            // ...
            .map(n -> n.substring(n.indexOf("[") + 1, n.indexOf("]")).replaceAll("[^0-9]", ""))
            .map(n -> Long.parseLong(n))  
            .reduce(0L, (a, b) -> a + b);

        Formatter formatter = IoC.getInstance().getFormatter();
        String totalfmt = formatter.fmtPrice(total, 1);     // Format: "1.00 EUR"
        System.out.println(String.format("%91s\n%91s\n%91s", 
            "-".repeat(13), totalfmt, "=".repeat(13)));
    }


    /**
     * Create objects from JSON array extracted from a JSON file.
     * 
     * @param jsonFileName name of JSON file to extract jsonProperty with JSON array.
     * @param jsonPropertyName name of JSON property to extract JSON array.
     * @param objCreator functional interface called to create object.
     * @return list of objects extracted from JSON array.
     */
    public List<String> createfromJSON(String jsonFileName, String jsonPropertyName,
        Function<JsonNode, Optional<String>> objCreator)
    {
        try(var fis = new FileInputStream(jsonFileName)) {
            // attempt to extract "payments" property as ArrayNode from JSON
            ArrayNode jsonArr = Optional.ofNullable(mapper.readTree(fis).get(jsonPropertyName))
                .filter(jsonNode -> jsonNode.isArray())	// if property is present, node must be ArrayNode
                .map(jsonNode -> (ArrayNode)jsonNode)	// then: cast JsonNode to ArrayNode
                .orElse(mapper.createArrayNode());		// return empty ArrayNode otherwise
            //
            // attempt to read objects from ArrayNode by invoking objCreator to create object
            return StreamSupport.stream(jsonArr.spliterator(), false)
                .map(jn -> objCreator.apply(jn))		// attempt to create object
                .filter(opt -> opt.isPresent())
                .map(opt -> opt.get())					// map Optional to object, if present
                .collect(Collectors.toList());
        //
        } catch(FileNotFoundException e) {
            System.err.println("File not found: " + jsonFileName);
        //
        } catch(Exception e) {
            e.printStackTrace();
        }
        return List.of();
    }


    /**
     * Attempt to create object from JsonNode properties. Returns empty Optional
     * in case of failure. Used as object creator in readJson().
     * 
     * @param jn JsonNode from which object is attempted to be created.
     * @return created object or empty Optional.
     */
    public Optional<String> createPayment(JsonNode jn) {
        // attempt to extract properties from JsonNode and create object
        String payment_id = jsonAsString(jn.get("payment_id"), " **** ***** ");
        long orderId = jsonAsLong(jn.get("order_id"), -1L);
        long paymentL = jsonAsLong(jn.get("payment"), -1L);
        String payment = String.format("%4d.%02d EUR", paymentL/100L, paymentL%100L);
        var recvdOpt = toLocalDateTime(dtf_json, jsonAsString(jn.get("received"), ""));
        long recvdL = recvdOpt.map(ldt -> toLong(ldt)).orElse(-1L);
        String recvdStr = recvdOpt.map(ldt -> toString(dtf_print, ldt)).orElse(" **** ");
        //
        // create object as String-object
        String obj = String.format("payment(%s) for order(%d) of [%s], received \"%s\"",
                payment_id, orderId, payment, recvdStr);
        //
        // check completeness and validity of extracted properties
        boolean valid = payment_id.startsWith("PY-");
        valid = valid && orderId >= 1000000000L && orderId <= 9999999999L;
        valid = valid && paymentL > 0;
        valid = valid && recvdL >= lower && recvdL < upper;
        //
        if( ! valid) {
            System.err.println("dropped (invalid payment): " + obj);
        }
        return Optional.ofNullable(valid? obj : null);
    }


    /**
     * Attempt to extract long value from JsonNode. Returns alternative
     * value when extraction from JsonNode was not possible.
     * 
     * @param jnode JsonNode from which value is extracted.
     * @param alt alternative value returned when extraction failed.
     * @return long value extracted from JsonNode or alternative value.
     */
    public long jsonAsLong(JsonNode jnode, long alt) {
        return Optional.ofNullable(jnode)
            .map(jn -> jn.asLong(alt)).orElse(alt);
    }


    /**
     * Attempt to extract String value from JsonNode. Returns alternative
     * String when extraction from JsonNode was not possible.
     * 
     * @param jnode JsonNode from which value is extracted.
     * @param alt alternative value returned when extraction failed.
     * @return String value extracted from JsonNode or alternative value.
     */
    public String jsonAsString(JsonNode jnode, String alt) {
        return Optional.ofNullable(jnode).map(jn -> jn.asText(alt)).orElse(alt);
    }


    /**
     * Attempt to parse LocalDateTime object from String. Returns empty Optional
     * in case of failure.
     * 
     * @param dtf DateTimeFormatter that defines the String format for parser.
     * @param datetimeStr String to convert.
     * @return LocalDateTime object or empty Optional.
     */
    public Optional<LocalDateTime> toLocalDateTime(DateTimeFormatter dtf, String datetimeStr) {
        try {
            return Optional.of(LocalDateTime.parse(datetimeStr, dtf));
        } catch(DateTimeParseException e) {}
        return Optional.empty();
    }


    /**
     * Convert LocalDateTime object to String representation defined by
     * DateTimeFormatter.
     * 
     * @param dtf DateTimeFormatter that defines the String representation.
     * @param ldt LocalDateTime object to convert.
     * @return String representation of LocalDateTime object.
     */
    public String toString(DateTimeFormatter dtf, LocalDateTime ldt) {
        return ldt.format(dtf);
    }


    /**
     * Convert LocalDateTime object to long value using epoch milliseconds,
     * which are milliseconds since 01/01/1970 00:00:00.
     * 
     * @param ldt LocalDateTime object to convert.
     * @return long value of LocalDateTime object.
     */
    public long toLong(LocalDateTime ldt) {
        return ldt.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
    }
}
