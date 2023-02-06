package fr.cyberal.postgrest;

public interface PostgrestCient {

    PostgrestQueryBuilder from(String table);

    PostgrestQueryBuilder rpc(String fnName);

    PostgrestCient user(String user);

    PostgrestCient password(String password);



}
