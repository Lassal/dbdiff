package br.lassal.dbvcs.tatubola;

public class DBEnvironmentInfo {

    private String environmentName;
    private String jdbcUrl;
    private String username;
    private String password;

    public DBEnvironmentInfo(String environmentName, String jdbcUrl, String username, String password){
        this.environmentName = environmentName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }


    public String getEnvironmentName() {
        return environmentName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
