package com.example.jms.web;

import com.example.jms.model.ActivityRecord;
import com.example.jms.model.BrowsedMessage;
import com.example.jms.model.MessageActivityStore;
import com.example.jms.service.DynamicAsyncListenerService;
import com.example.jms.service.JmsService;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * REST-like Servlet endpoint providing JSON APIs for JMS operations.
 */
@WebServlet(name = "JmsApiController", urlPatterns = {"/api/jms/*"})
public class JmsApiController extends HttpServlet {

    private static final Logger LOGGER = Logger.getLogger(JmsApiController.class.getName());
    private static final String DEFAULT_CF = "ConnectionFactory";
    private static final String DEFAULT_QUEUE = "ExamplesQueue";

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        setSecurityHeaders(resp);
        String path = getSubPath(req);

        try {
            if ("/status".equals(path)) {
                handleStatus(resp);
            } else if ("/activities".equals(path)) {
                handleGetActivities(resp);
            } else if ("/queue/browse".equals(path)) {
                handleBrowseQueue(req, resp);
            } else {
                sendErrorJson(resp, HttpServletResponse.SC_NOT_FOUND, "Endpoint not found: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error handling GET " + path, e);
            sendErrorJson(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        setSecurityHeaders(resp);
        String path = getSubPath(req);

        try {
            if ("/test-jndi".equals(path)) {
                handleTestJndi(req, resp);
            } else if ("/queue/send".equals(path)) {
                handleSendQueue(req, resp);
            } else if ("/queue/receive".equals(path)) {
                handleReceiveQueue(req, resp);
            } else if ("/topic/publish".equals(path)) {
                handlePublishTopic(req, resp);
            } else if ("/listener/start".equals(path)) {
                handleStartListener(req, resp);
            } else if ("/listener/stop".equals(path)) {
                handleStopListener(resp);
            } else if ("/activities/clear".equals(path)) {
                handleClearActivities(resp);
            } else {
                sendErrorJson(resp, HttpServletResponse.SC_NOT_FOUND, "Endpoint not found: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error handling POST " + path, e);
            sendErrorJson(resp, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    private void handleStatus(HttpServletResponse resp) throws IOException {
        DynamicAsyncListenerService listener = DynamicAsyncListenerService.getInstance();
        JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("success", true)
                .add("listenerRunning", listener.isRunning())
                .add("listeningDestination", listener.getCurrentDestination() != null ? listener.getCurrentDestination() : "")
                .add("defaultCf", DEFAULT_CF)
                .add("defaultQueue", DEFAULT_QUEUE);

        writeJson(resp, builder.build());
    }

    private void handleTestJndi(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        JsonObject body = parseJsonBody(req);
        String cfJndi = sanitizeJndi(body.getString("cfJndi", DEFAULT_CF));
        String destJndi = body.containsKey("destJndi") ? body.getString("destJndi").trim() : "";

        Map<String, Object> testResult = JmsService.getInstance().testJndi(cfJndi, destJndi);

        JsonObjectBuilder builder = Json.createObjectBuilder()
                .add("success", (Boolean) testResult.get("success"))
                .add("message", (String) testResult.getOrDefault("message", ""))
                .add("error", (String) testResult.getOrDefault("error", ""))
                .add("cfFound", (Boolean) testResult.getOrDefault("cfFound", false))
                .add("destFound", (Boolean) testResult.getOrDefault("destFound", false))
                .add("cfType", (String) testResult.getOrDefault("cfType", ""))
                .add("destType", (String) testResult.getOrDefault("destType", ""))
                .add("destCategory", (String) testResult.getOrDefault("destCategory", ""));

        writeJson(resp, builder.build());
    }

    private void handleSendQueue(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        JsonObject body = parseJsonBody(req);
        String cfJndi = sanitizeJndi(body.getString("cfJndi", DEFAULT_CF));
        String destJndi = sanitizeJndi(body.getString("destJndi", DEFAULT_QUEUE));
        String payload = body.getString("payload", "Test message from JEUS JMS Console");
        int priority = body.getInt("priority", 4);
        long ttl = body.getInt("timeToLive", 0);
        String corrId = body.getString("correlationId", "");
        int count = body.getInt("count", 1);

        Map<String, String> properties = parseProperties(body);

        List<String> msgIds = JmsService.getInstance().sendQueueMessage(
                cfJndi, destJndi, payload, priority, ttl, corrId, properties, count
        );

        JsonArrayBuilder idsArray = Json.createArrayBuilder();
        for (String id : msgIds) {
            idsArray.add(id);
        }

        JsonObject responseObj = Json.createObjectBuilder()
                .add("success", true)
                .add("sentCount", msgIds.size())
                .add("messageIds", idsArray)
                .build();

        writeJson(resp, responseObj);
    }

    private void handleBrowseQueue(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        String cfJndi = sanitizeJndi(req.getParameter("cfJndi"));
        String destJndi = sanitizeJndi(req.getParameter("destJndi"));
        String selector = req.getParameter("selector");

        if (cfJndi.isEmpty()) cfJndi = DEFAULT_CF;
        if (destJndi.isEmpty()) destJndi = DEFAULT_QUEUE;

        List<BrowsedMessage> messages = JmsService.getInstance().browseQueue(cfJndi, destJndi, selector);

        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();
        for (BrowsedMessage msg : messages) {
            JsonObjectBuilder msgObj = Json.createObjectBuilder()
                    .add("messageId", msg.getMessageId())
                    .add("correlationId", msg.getCorrelationId())
                    .add("timestamp", msg.getTimestamp())
                    .add("priority", msg.getPriority())
                    .add("deliveryMode", msg.getDeliveryMode())
                    .add("expiration", msg.getExpiration())
                    .add("redelivered", msg.isRedelivered())
                    .add("payload", msg.getPayload());

            JsonObjectBuilder propsObj = Json.createObjectBuilder();
            for (Map.Entry<String, String> entry : msg.getProperties().entrySet()) {
                propsObj.add(entry.getKey(), entry.getValue());
            }
            msgObj.add("properties", propsObj);

            arrayBuilder.add(msgObj);
        }

        JsonObject responseObj = Json.createObjectBuilder()
                .add("success", true)
                .add("count", messages.size())
                .add("messages", arrayBuilder)
                .build();

        writeJson(resp, responseObj);
    }

    private void handleReceiveQueue(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        JsonObject body = parseJsonBody(req);
        String cfJndi = sanitizeJndi(body.getString("cfJndi", DEFAULT_CF));
        String destJndi = sanitizeJndi(body.getString("destJndi", DEFAULT_QUEUE));
        long timeout = body.getInt("timeout", 2000);
        String selector = body.getString("selector", "");

        BrowsedMessage msg = JmsService.getInstance().receiveQueueMessage(cfJndi, destJndi, timeout, selector);

        JsonObjectBuilder responseBuilder = Json.createObjectBuilder();
        responseBuilder.add("success", true);

        if (msg != null) {
            responseBuilder.add("empty", false);
            JsonObjectBuilder msgObj = Json.createObjectBuilder()
                    .add("messageId", msg.getMessageId())
                    .add("correlationId", msg.getCorrelationId())
                    .add("timestamp", msg.getTimestamp())
                    .add("priority", msg.getPriority())
                    .add("payload", msg.getPayload());

            JsonObjectBuilder propsObj = Json.createObjectBuilder();
            for (Map.Entry<String, String> entry : msg.getProperties().entrySet()) {
                propsObj.add(entry.getKey(), entry.getValue());
            }
            msgObj.add("properties", propsObj);
            responseBuilder.add("message", msgObj);
        } else {
            responseBuilder.add("empty", true);
            responseBuilder.add("details", "Queue is empty or wait timed out (" + timeout + "ms)");
        }

        writeJson(resp, responseBuilder.build());
    }

    private void handlePublishTopic(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        JsonObject body = parseJsonBody(req);
        String cfJndi = sanitizeJndi(body.getString("cfJndi", DEFAULT_CF));
        String topicJndi = sanitizeJndi(body.getString("destJndi", "jms/TestTopic"));
        String payload = body.getString("payload", "Broadcast event from JEUS JMS Console");
        int priority = body.getInt("priority", 4);
        long ttl = body.getInt("timeToLive", 0);
        String corrId = body.getString("correlationId", "");

        Map<String, String> properties = parseProperties(body);

        String msgId = JmsService.getInstance().publishTopicMessage(
                cfJndi, topicJndi, payload, priority, ttl, corrId, properties
        );

        JsonObject responseObj = Json.createObjectBuilder()
                .add("success", true)
                .add("messageId", msgId)
                .build();

        writeJson(resp, responseObj);
    }

    private void handleStartListener(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        JsonObject body = parseJsonBody(req);
        String cfJndi = sanitizeJndi(body.getString("cfJndi", DEFAULT_CF));
        String destJndi = sanitizeJndi(body.getString("destJndi", DEFAULT_QUEUE));

        DynamicAsyncListenerService.getInstance().start(cfJndi, destJndi);

        JsonObject responseObj = Json.createObjectBuilder()
                .add("success", true)
                .add("listenerRunning", true)
                .add("listeningDestination", destJndi)
                .build();

        writeJson(resp, responseObj);
    }

    private void handleStopListener(HttpServletResponse resp) throws IOException {
        DynamicAsyncListenerService.getInstance().stop();

        JsonObject responseObj = Json.createObjectBuilder()
                .add("success", true)
                .add("listenerRunning", false)
                .build();

        writeJson(resp, responseObj);
    }

    private void handleGetActivities(HttpServletResponse resp) throws IOException {
        List<ActivityRecord> list = MessageActivityStore.getInstance().getActivities();
        JsonArrayBuilder arrayBuilder = Json.createArrayBuilder();

        for (ActivityRecord rec : list) {
            JsonObjectBuilder obj = Json.createObjectBuilder()
                    .add("id", rec.getId())
                    .add("timestamp", rec.getTimestamp())
                    .add("action", rec.getAction())
                    .add("destination", rec.getDestination())
                    .add("messageId", rec.getMessageId())
                    .add("correlationId", rec.getCorrelationId())
                    .add("priority", rec.getPriority())
                    .add("payload", rec.getPayload())
                    .add("status", rec.getStatus())
                    .add("details", rec.getDetails());

            JsonObjectBuilder propsObj = Json.createObjectBuilder();
            for (Map.Entry<String, String> entry : rec.getProperties().entrySet()) {
                propsObj.add(entry.getKey(), entry.getValue());
            }
            obj.add("properties", propsObj);

            arrayBuilder.add(obj);
        }

        JsonObject responseObj = Json.createObjectBuilder()
                .add("success", true)
                .add("activities", arrayBuilder)
                .build();

        writeJson(resp, responseObj);
    }

    private void handleClearActivities(HttpServletResponse resp) throws IOException {
        MessageActivityStore.getInstance().clear();
        JsonObject responseObj = Json.createObjectBuilder()
                .add("success", true)
                .build();
        writeJson(resp, responseObj);
    }

    private Map<String, String> parseProperties(JsonObject body) {
        Map<String, String> map = new HashMap<>();
        if (body.containsKey("properties") && body.get("properties").getValueType() == JsonValue.ValueType.OBJECT) {
            JsonObject propsObj = body.getJsonObject("properties");
            for (String key : propsObj.keySet()) {
                map.put(key, propsObj.getString(key, ""));
            }
        }
        return map;
    }

    private JsonObject parseJsonBody(HttpServletRequest req) throws IOException {
        try (JsonReader reader = Json.createReader(new InputStreamReader(req.getInputStream(), StandardCharsets.UTF_8))) {
            return reader.readObject();
        } catch (Exception e) {
            return Json.createObjectBuilder().build();
        }
    }

    private String getSubPath(HttpServletRequest req) {
        String pathInfo = req.getPathInfo();
        return (pathInfo != null) ? pathInfo : "";
    }

    private String sanitizeJndi(String input) {
        if (input == null) return "";
        return input.trim();
    }

    private void writeJson(HttpServletResponse resp, JsonObject json) throws IOException {
        resp.setContentType("application/json; charset=UTF-8");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().write(json.toString());
    }

    private void sendErrorJson(HttpServletResponse resp, int status, String message) throws IOException {
        resp.setContentType("application/json; charset=UTF-8");
        resp.setStatus(status);
        JsonObject errorObj = Json.createObjectBuilder()
                .add("success", false)
                .add("error", message != null ? message : "Internal server error")
                .build();
        resp.getWriter().write(errorObj.toString());
    }

    private void setSecurityHeaders(HttpServletResponse resp) {
        resp.setHeader("X-Content-Type-Options", "nosniff");
        resp.setHeader("X-Frame-Options", "SAMEORIGIN");
        resp.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
    }
}
