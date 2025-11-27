package com.sptech.school;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

public class EtlDashboardBateria implements RequestHandler<S3Event, String> {

    // Criação do cliente S3
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    // Nome do bucket e arquivos de destino
    private static final String BUCKET_CLIENT = "s3clientnavix-20251107102936-7403";
    private static final String OUTPUT_FILENAME = "dashboard_bateria.csv";
    private static final String HISTORICO_FILENAME = "historico_leituras.csv";

    // Classe auxiliar para agrupar dados
    static class ModelStats {
        double somaSaude = 0;
        double somaCarga = 0;
        double somaTemp = 0;
        int count = 0;
        int alertasCount = 0;
    }

    // --- MÉTODOS DE CONEXÃO COM O BANCO DE DADOS---

    // Busca todos os IDs e Nomes dos modelos existentes no banco de dados para o sorteio
    private Map<String, String> buscarModelosDisponiveis(Context context) {
        Map<String, String> modelos = new HashMap<>();

        Conexao conexao = new Conexao();
        Connection conn = conexao.getConexao();

        if (conn == null) {
            context.getLogger().log("ERRO: Falha na conexão para buscar modelos disponíveis.");
            return modelos;
        }

        String sql = "SELECT id, nome FROM modelo WHERE mac_address IS NULL OR mac_address = '';";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                modelos.put(rs.getString("id"), rs.getString("nome"));
            }
        } catch (Exception e) {
            context.getLogger().log("Erro ao buscar modelos disponíveis no DB: " + e.getMessage());
        } finally {
            conexao.fecharConexao();
        }
        return modelos;
    }

    // Se não encontrar um modelo sorteia um modelo sem MAC na lista
    private Map<String, String> buscarOuCriarModeloPorMac(String mac, Context context, Map<String, String> modelosDisponiveis) {
        Conexao conexao = new Conexao();
        Connection conn = conexao.getConexao();
        Map<String, String> resultado = new HashMap<>();

        if (conn == null) return resultado;

        try {
            // 1. Tentar Buscar o Modelo pelo MAC
            String sqlSelect = "SELECT id, nome FROM modelo WHERE mac_address = ?;";
            java.sql.PreparedStatement ps = conn.prepareStatement(sqlSelect);
            ps.setString(1, mac);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                // Se SIM: Retorna o Modelo
                resultado.put("id", rs.getString("id"));
                resultado.put("nome", rs.getString("nome"));
                return resultado;
            }

            // 2. Se NÃO: Sorteia e Insere (Apenas se houver modelos disponíveis sem MAC)
            if (modelosDisponiveis.isEmpty()) {
                context.getLogger().log("Aviso: Nenhum modelo disponível para sorteio/associação. MAC não mapeado.");
                return resultado;
            }

            // Sorteio de um ID de modelo disponível
            List<String> idsDisponiveis = new ArrayList<>(modelosDisponiveis.keySet());
            Random rand = new Random();
            String idSorteado = idsDisponiveis.get(rand.nextInt(idsDisponiveis.size()));
            String nomeSorteado = modelosDisponiveis.get(idSorteado);

            // 3. Inserir o novo par (MAC + ID do Modelo Sorteado) no BD
            String sqlUpdate = "UPDATE modelo SET mac_address = ? WHERE id = ? AND (mac_address IS NULL OR mac_address = '');";
            java.sql.PreparedStatement psUpdate = conn.prepareStatement(sqlUpdate);
            psUpdate.setString(1, mac);
            psUpdate.setString(2, idSorteado);

            int rowsUpdated = psUpdate.executeUpdate();

            if (rowsUpdated > 0) {
                context.getLogger().log("Novo MAC (" + mac + ") associado ao Modelo Sorteado: " + nomeSorteado);
                modelosDisponiveis.remove(idSorteado);
            } else {
                context.getLogger().log("Aviso: Tentativa de associação falhou. Modelo sorteado (" + idSorteado + ") já tem um MAC.");
            }

            resultado.put("id", idSorteado);
            resultado.put("nome", nomeSorteado);


        } catch (Exception e) {
            context.getLogger().log("Erro ao buscar/criar modelo por MAC: " + e.getMessage());
        } finally {
            conexao.fecharConexao();
        }
        return resultado;
    }

    // --- METODO PRINCIPAL DA LAMBDA ---
    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        // 0. Inicializa os modelos disponíveis
        Map<String, String> modelosDisponiveis = buscarModelosDisponiveis(context);

        // Mapa para agrupar os dados por MODELO (CSV de Dashboard)
        Map<String, ModelStats> statsPorModelo = new HashMap<>();

        // StringBuilder para armazenar os dados brutos (CSV de Histórico)
        StringBuilder csvHistoricoOutput = new StringBuilder();

        // Cabeçalho do CSV de histórico
        csvHistoricoOutput.append("MAC,Modelo,TIMESTAMP,velocidadeEstimada,consumoEnergia,TEMP\n");

        try {
            // 1. Obter informações do evento S3 bucket trusted
            String sourceBucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
            String sourceKey = s3Event.getRecords().get(0).getS3().getObject().getKey();

            context.getLogger().log("Iniciando processamento do arquivo: " + sourceKey);

            // 2. Ler o arquivo CSV
            InputStream s3InputStream = s3Client.getObject(sourceBucket, sourceKey).getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3InputStream, StandardCharsets.UTF_8));

            String linha;
            boolean primeiraLinha = true;

            while ((linha = reader.readLine()) != null) {
                if (primeiraLinha) { primeiraLinha = false; continue; }

                String[] cols = linha.split(",");
                if (cols.length < 15) continue;

                // --- Lógica MAC -> Modelo ---
                String macDispositivo = cols[1].trim();

                Map<String, String> modeloAssociado =
                        buscarOuCriarModeloPorMac(macDispositivo, context, modelosDisponiveis);

                if (!modeloAssociado.containsKey("nome")) {
                    continue;
                }

                String nomeModelo = modeloAssociado.get("nome");
                // Fim da Lógica MAC -> Modelo

                // Extração dos dados
                try {
                    // Dados para Agregação (Dashboard)
                    double saude = Double.parseDouble(cols[6].trim()); // BATERIA
                    double carga = Double.parseDouble(cols[3].trim()); // RAM
                    double temp = Double.parseDouble(cols[7].trim());   // TEMP

                    // Dados para Histórico (Gráficos de Linha e Histograma)
                    String timestamp = cols[0].trim();
                    String velocidade = cols[9].trim(); // velocidadeEstimada
                    String consumo = cols[10].trim();    // consumoEnergia
                    String tempBruta = cols[7].trim();   // TEMP bruta

                    // --- Agrupamento e Lógica (CSV Agregado) ---
                    statsPorModelo.putIfAbsent(nomeModelo, new ModelStats());
                    ModelStats stats = statsPorModelo.get(nomeModelo);

                    stats.somaSaude += saude;
                    stats.somaCarga += carga;
                    stats.somaTemp += temp;
                    stats.count++;

                    if (carga < 20 || temp > 45 || saude < 70) {
                        stats.alertasCount++;
                    }

                    // --- Coleta de Dados Brutos (CSV Histórico) ---
                    csvHistoricoOutput.append(String.format("%s,%s,%s,%s,%s,%s\n",
                            macDispositivo, nomeModelo, timestamp, velocidade, consumo, tempBruta));

                } catch (NumberFormatException e) {
                    context.getLogger().log("Erro de formato numérico na linha: " + linha);
                    continue;
                }
            }
            reader.close();

            // 3. Gerar o CSV de Saída Agregado (Dashboard)
            StringBuilder csvOutput = new StringBuilder();
            csvOutput.append("Modelo,Media_Saude,Media_Carga,Media_Temperatura,Qtd_Veiculos,Qtd_Alertas,Status_Geral\n");

            for (Map.Entry<String, ModelStats> entry : statsPorModelo.entrySet()) {
                String nomeModelo = entry.getKey();
                ModelStats s = entry.getValue();

                double mediaSaude = s.somaSaude / s.count;
                double mediaCarga = s.somaCarga / s.count;
                double mediaTemp = s.somaTemp / s.count;
                String statusGeral = (mediaTemp > 45 || mediaCarga < 20) ? "CRITICO" : "NORMAL";

                csvOutput.append(String.format(Locale.US, "%s,%.2f,%.2f,%.2f,%d,%d,%s\n",
                        nomeModelo, mediaSaude, mediaCarga, mediaTemp, s.count, s.alertasCount, statusGeral));
            }

            // 4. Preparar e Enviar para o Bucket de Destino Client

            // 4.1. Envio do CSV de Dashboard
            byte[] bytesOutput = csvOutput.toString().getBytes(StandardCharsets.UTF_8);
            InputStream inputStreamUpload = new ByteArrayInputStream(bytesOutput);

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("text/csv");
            metadata.setContentLength(bytesOutput.length);

            String pastaDestino = "dashBateria/";

            s3Client.putObject(
                    BUCKET_CLIENT,
                    pastaDestino + OUTPUT_FILENAME,
                    inputStreamUpload,
                    metadata
            );            context.getLogger().log("Dashboard gerado com sucesso em: " + BUCKET_CLIENT + "/" + pastaDestino + OUTPUT_FILENAME);

            // 4.2. Envio do CSV de Histórico
            byte[] bytesHistoricoOutput = csvHistoricoOutput.toString().getBytes(StandardCharsets.UTF_8);
            InputStream inputStreamHistoricoUpload = new ByteArrayInputStream(bytesHistoricoOutput);

            ObjectMetadata metadataHistorico = new ObjectMetadata();
            metadataHistorico.setContentType("text/csv");
            metadataHistorico.setContentLength(bytesHistoricoOutput.length);

            s3Client.putObject(
                    BUCKET_CLIENT,
                    pastaDestino + HISTORICO_FILENAME,
                    inputStreamHistoricoUpload,
                    metadataHistorico
            );            context.getLogger().log("Histórico de Leituras gerado com sucesso em: " + BUCKET_CLIENT + "/" + HISTORICO_FILENAME);


            return "Sucesso: Dashboard e Histórico atualizados.";

        } catch (Exception e) {
            context.getLogger().log("Erro fatal no processamento: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}