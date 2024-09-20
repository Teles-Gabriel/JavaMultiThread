package threads;

import java.io.*;
import java.util.*;

public class TarefaProcessamento implements Runnable {
    private List<String> cidades;
    private String caminhoArquivos = "./temperaturas_cidades";

    public TarefaProcessamento(List<String> cidades) {
        this.cidades = cidades;
    }

    @Override
    public void run() {
        for (String cidade : cidades) {
            String caminhoArquivo = caminhoArquivos + cidade + ".csv"; // Supondo que o arquivo seja nomeado pela cidade
            File arquivoCSV = new File(caminhoArquivo);

            if (arquivoCSV.exists()) {
                processarArquivoCSV(caminhoArquivo);
            } else {
                System.out.println("Arquivo CSV não encontrado para a cidade: " + cidade);
            }
        }
    }

    private void processarArquivoCSV(String caminhoArquivo) {
        // Listas para armazenar as temperaturas
        List<Double> temperaturas = new ArrayList<>();
        
        try (BufferedReader br = new BufferedReader(new FileReader(caminhoArquivo))) {
            String linha;
            while ((linha = br.readLine()) != null) {
                String[] valores = linha.split(",");  // Supondo que o arquivo CSV use vírgulas para separação
                try {
                    double temperatura = Double.parseDouble(valores[1]);  // Pega a coluna de temperatura
                    temperaturas.add(temperatura);
                } catch (NumberFormatException e) {
                    System.out.println("Erro ao converter temperatura no arquivo: " + caminhoArquivo);
                }
            }

            // Calcular média, máxima e mínima
            double media = calcularMedia(temperaturas);
            double maxima = Collections.max(temperaturas);
            double minima = Collections.min(temperaturas);

            System.out.println("Cidade: " + caminhoArquivo);
            System.out.println("Temperatura Média: " + media);
            System.out.println("Temperatura Máxima: " + maxima);
            System.out.println("Temperatura Mínima: " + minima);
            System.out.println("------------------------------------");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private double calcularMedia(List<Double> temperaturas) {
        double soma = 0.0;
        for (double temp : temperaturas) {
            soma += temp;
        }
        return soma / temperaturas.size();
    }
}
