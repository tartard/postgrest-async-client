package fr.cyberal.postgrest;

import java.net.http.HttpRequest;
import java.util.*;

class PostgrestQueryBuilder {

    // Mimetypes are stored as string not ot prevent the use of other mimetype which could be possibly supported by
    // postgrest in the future .
    static final String JSON_MIMETYPE = "application/json";
    static final String XML_MIMETYPE = "text/xm";
    static final String OCTET_STREAM_MIMETYPE = "application/octet-stream";
    static final String CSV = "text/csv";


    // Some headers are stored as filed to prevent from setting these headers several times .

    private String baseUri;
    private String jwt;
    private String path;
    private Map<String, String> params = new HashMap<>();
    private Set<String> rawParams = new HashSet<>();
    private HttpMethod httpMethod;
    private String schema;
    private Map<String, String> headers = new HashMap<>();
    private Object body;
    private boolean singleResult;
    private boolean returnRepresentation;
    private CountPreference countPreference;
    private Integer offset;
    private Integer limit;

    PostgrestQueryBuilder(String baseUri, String jwt) {
        this.baseUri = baseUri;
        this.jwt = jwt;
    }

    PostgrestQueryBuilder(String baseUri, String jwt, String path) {
        this(baseUri, jwt);
        this.path = path;
    }

    PostgrestQueryBuilder from(String table) {
        this.path = "/" + table;
        return this;
    }

    PostgrestQueryBuilder rpc(String rpc) {
        this.path = "/rpc/" + rpc;
        this.setHttpMethod(HttpMethod.POST);
        return this;
    }

    PostgrestQueryBuilder rpc(String rpc, Object body) {
        this.rpc(rpc);
        this.body(body);
        this.json();
        return this;
    }

    PostgrestQueryBuilder rpc(String rpc, Object body, boolean singleObject) {
        this.rpc(rpc, body);
        this.param("Prefer", "params=single-object");
        return this;
    }

    PostgrestQueryBuilder bulkRpc(String rpc, String body) {
        this.rpc(rpc, body);
        this.contentType(CSV);
        this.prefer("params=multiple-objects");
        return this;
    }

    
    PostgrestQueryBuilder readOnlyRpc(String rpc) {
        this.rpc(rpc);
        this.setHttpMethod(HttpMethod.GET);
        return this;
    }

    PostgrestQueryBuilder readOnlyRpc(String rpc, String ...filters) {
        this.readOnlyRpc(rpc);
        for(String filter : filters) {
            this.rawParam(filter);
        }
        return this;
    }

    PostgrestQueryBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    PostgrestQueryBuilder select() {
        return setHttpMethod(HttpMethod.GET);
    }

    PostgrestQueryBuilder select(String columns) {
        return setHttpMethod(HttpMethod.GET)
                .param("select", columns);
    }

    PostgrestQueryBuilder insert(Object body) {
        setHttpMethod(HttpMethod.POST);
        this.body(body);
        return this;
    }

    PostgrestQueryBuilder insert(String columnsToInsert, Object body) {
        return this.insert(body).param("columns", columnsToInsert).returnRepresentation();
    }

    PostgrestQueryBuilder returnedColumns(String returnedColumns) {
        return this.param("select", returnedColumns);
    }

    PostgrestQueryBuilder update(Object body) {
        setHttpMethod(HttpMethod.PATCH);
        this.body(body);
        return this.returnRepresentation();
    }

    PostgrestQueryBuilder update(String rawFilter, Object body) {
        return this.update(body).rawParam(rawFilter);
    }

    PostgrestQueryBuilder upsert(Object body) {
        return this.upsert(body, true);
    }

    PostgrestQueryBuilder upsert(Object body, String filter) {
        this.body(body);
        this.setHttpMethod(HttpMethod.PUT);
        return this.param("on_conflict", onConflict);
    }

    PostgrestQueryBuilder upsert(Object body, boolean mergeDuplicates) {
        setHttpMethod(HttpMethod.POST);
        this.body(body);
        return this.prefer(mergeDuplicates ? "resolution=merge-duplicates" : "resolution=ignore-duplicates");
    }



    PostgrestQueryBuilder delete() {
        return setHttpMethod(HttpMethod.DELETE);
    }

    PostgrestQueryBuilder delete(String ...filters) {
        for(String filter : filters) {
            this.rawParam(filter);
        }
        return delete();
    }

    PostgrestQueryBuilder body(Object body) {
        this.body = body;
        return this;
    }

    PostgrestQueryBuilder prefer(String preference) {
        return this.header("Prefer", preference);
    }

    private PostgrestQueryBuilder setHttpMethod(HttpMethod httpMethod) {
        /*f(this.httpMethod != null) {
            throw new UnsupportedOperationException("HttMethod is already set");
        }*/
        this.httpMethod = httpMethod;
        return this;
    }

    PostgrestQueryBuilder param(String name, String value) {
        this.params.put(name, value);
        return this;
    }

    PostgrestQueryBuilder rawParam(String filter) {
        this.rawParams.add(filter);
        return this;
    }

    PostgrestQueryBuilder limit(int limit) {
        this.limit = limit;
        return this;
    }

    PostgrestQueryBuilder offset(int offset) {
        this.offset = offset;
        return this;
    }

    PostgrestQueryBuilder header(String key, String value) {
        headers.put(key, value);
        return this;
    }

    PostgrestQueryBuilder contentType(String contentType) {
        return this.header("Content-Type", contentType);
    }

    PostgrestQueryBuilder json() {
        return this.contentType(JSON_MIMETYPE);
    }

    PostgrestQueryBuilder xml() {
        return this.contentType(XML_MIMETYPE);
    }

    PostgrestQueryBuilder octetStream() {
        return this.contentType(OCTET_STREAM_MIMETYPE);
    }

    PostgrestQueryBuilder accept(String mimeType) {
        return this.header("Accept", mimeType);
    }

    enum CountPreference {
        /**
         * In order to obtain the total size of the table or view (such as when rendering the last page link in a pagination control), specify Prefer: count=exact as a request header.
         * Note that the larger the table the slower this query runs in the database. The server will respond with the selected range and total.
         */
        EXACT,
        /**
         * To avoid the shortcomings of exact count, PostgREST can leverage PostgreSQL statistics and get a fairly accurate and fast count. To do this, specify the Prefer: count=planned header.
         * Note that the accuracy of this count depends on how up-to-date are the PostgreSQL statistics tables. For example in this case, to increase the accuracy of the count you can do ANALYZE bigtable. See ANALYZE for more details.
         */
        PLANNED,
        /**
         * When you are interested in the count, the relative error is important. If you have a planned count of 1000000 and the exact count is 1001000, the error is small enough to be ignored. But with a planned count of 7, an exact count of 28 would be a huge misprediction.
         *
         * In general, when having smaller row-counts, the estimated count should be as close to the exact count as possible.
         *
         * To help with these cases, PostgREST can get the exact count up until a threshold and get the planned count when that threshold is surpassed. To use this behavior, you can specify the Prefer: count=estimated header. The threshold is defined by db-max-rows.
         */
        ESTIMATED
    }

    PostgrestQueryBuilder countPref(CountPreference countPreference) {
        this.countPreference = countPreference;
        return this;
    }

    PostgrestQueryBuilder singleResult() {
        this.singleResult = true;
        return this;
    }

    PostgrestQueryBuilder returnRepresentation() {
        this.returnRepresentation = true;
        return this;
    }



    private HttpRequest build() {
        Map.of("schema", schema, "path", path, "method", httpMethod).forEach((key, value) -> {
            if (value == null) {
                throw new NullPointerException("Missing mandatory field : " + key);
            }
        });
        HttpRequest.Builder reqBuilder = HttpRequest.newBuilder();

        if(this.httpMethod.equals(HttpMethod.GET)) {
            reqBuilder.header("Accept-Profile", schema);
        }
        else {
            reqBuilder.header("Content-Profile", schema);
        }
        if(this.singleResult) {
            this.header("Accept", "application/vnd.pgrst.object+json");
        }
        if(this.returnRepresentation) {
            this.header("Prefer", "return=representation");
        }
        if(this.countPreference != null) {
            this.header("Prefer", "count=" + countPreference.name().toLowerCase());
        }
        if(this.limit != null) {
            this.param("limit", Integer.toString(limit));
        }
        if(this.offset != null) {
            this.param("offset", Integer.toString(offset));
        }
        headers.entrySet().forEach(entry -> reqBuilder.header(entry.getKey(), entry.getValue()));
        reqBuilder.uri();
        reqBuilder.method()
    }


    enum HttpMethod {

        GET,
        PUT,
        POST,
        DELETE,
        PATCH
    }


}
