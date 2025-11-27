package com.sptech.school;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.Instance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Conexao {
    private Connection conexao;

    public Conexao() {
        try {
            Ec2Client ec2 = Ec2Client.builder()
                    .region(Region.US_EAST_1)
                    .build();
            String nomeInstancia = System.getenv("NOME_INSTANCIA");

            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(Filter.builder()
                            .name("tag:Name")
                            .values(nomeInstancia)
                            .build())
                    .build();

            DescribeInstancesResponse response = ec2.describeInstances(request);

            String ipPublico = null;
            for (var reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    ipPublico = instance.publicIpAddress();
                }
            }

            if (ipPublico == null) throw new RuntimeException("Nenhum IP encontrado para " + nomeInstancia);

            String url = "jdbc:mysql://" + ipPublico + ":3306/navix";
            String user = System.getenv("USER");
            String pass = System.getenv("SENHA");

            conexao = DriverManager.getConnection(url, user, pass);
            System.out.println("Conexão estabelecida com " + ipPublico);

        } catch (Exception e) {
            System.out.println("Erro conexão: " + e.getMessage());
            e.printStackTrace();
        }

    }


    public void fecharConexao() {
        try {
            if (conexao != null && !conexao.isClosed()) conexao.close();
        } catch (SQLException e) {
            System.out.println("Erro fechar: " + e.getMessage());
        }
    }

    public Connection getConexao() {
        return conexao;
    }
}

