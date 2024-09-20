package threads;

import java.util.concurrent.*;
import java.util.*;
import java.io.*;
import java.text.DecimalFormat;

public class ProcessadorTemperaturas {

    public static void main(String[] args) throws InterruptedException {
        int numRodadas = 10; // Número de rodadas
        int numThreads = 320; // Ajuste este valor conforme a versão
        List<Long> temposExecucao = new ArrayList<>(); // Lista para armazenar os tempos de cada rodada

        // Executar o experimento por 10 rodadas
        for (int rodada = 1; rodada <= numRodadas; rodada++) {
            // Medir o tempo de início da rodada
            long inicioTempo = System.currentTimeMillis();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            // Obter a lista de arquivos CSV na pasta "temperaturas_cidades"
            List<File> listaArquivos = obterArquivosCSV("./temperaturas_cidades/temperaturas_cidades");

            if (listaArquivos.isEmpty()) {
                System.out.println("Nenhum arquivo CSV encontrado para processar.");
                return;
            }

            // Divisão dos arquivos entre as threads
            int tamanhoGrupo = Math.max(1, listaArquivos.size() / numThreads); // Evitar divisão por zero, garante que sempre tenha ao menos 1 arquivo por thread
            for (int i = 0; i < numThreads; i++) {
                int inicio = i * tamanhoGrupo;
                int fim = (i == numThreads - 1) ? listaArquivos.size() : Math.min(inicio + tamanhoGrupo, listaArquivos.size());

                // Se não há mais arquivos, termina a divisão
                if (inicio >= listaArquivos.size()) break;

                List<File> subLista = listaArquivos.subList(inicio, fim);
                executor.execute(new TarefaProcessamento(subLista));
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            // Medir o tempo de fim da rodada
            long fimTempo = System.currentTimeMillis();
            long tempoRodada = fimTempo - inicioTempo;
            temposExecucao.add(tempoRodada); // Armazenar o tempo da rodada

            System.out.println("Tempo de execucao da rodada " + rodada + ": " + tempoRodada + " ms");
        }

        // Calcular o tempo médio de execução das 10 rodadas
        long somaTempos = 0;
        for (long tempo : temposExecucao) {
            somaTempos += tempo;
        }
        long tempoMedio = somaTempos / numRodadas;

        System.out.println("Tempo medio de execucao: " + tempoMedio + " ms");

        // Salvar os tempos de cada rodada e o tempo médio em um arquivo de texto
        salvarTemposExecucao(temposExecucao, tempoMedio, "versao_experimento.txt");
    }

    // Método para obter a lista de arquivos CSV da pasta "temperaturas_cidades"
    public static List<File> obterArquivosCSV(String diretorio) {
        List<File> arquivosCSV = new ArrayList<>();
        File pasta = new File(diretorio);

        // Verificar se o diretório existe e se é uma pasta
        if (pasta.exists() && pasta.isDirectory()) {
            File[] arquivos = pasta.listFiles((dir, nome) -> nome.toLowerCase().endsWith(".csv")); // Filtra apenas arquivos .csv
            if (arquivos != null) {
                arquivosCSV.addAll(Arrays.asList(arquivos)); // Adiciona todos os arquivos CSV à lista
            }
        } else {
            System.out.println("Diretorio nao encontrado: " + diretorio);
        }

        return arquivosCSV;
    }

    // Método para salvar os tempos de execução em um arquivo de texto
    public static void salvarTemposExecucao(List<Long> tempos, long tempoMedio, String nomeArquivo) {
        try {
            // Criar o arquivo de saída
            File arquivoSaida = new File(nomeArquivo);

            // Escrever os tempos no arquivo de texto
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(arquivoSaida))) {
                for (int i = 0; i < tempos.size(); i++) {
                    writer.write("Tempo da rodada " + (i + 1) + ": " + tempos.get(i) + " ms\n");
                }
                writer.write("Tempo medio de execucao: " + tempoMedio + " ms\n");
            }

            System.out.println("Tempos de execucao salvos em " + nomeArquivo);

        } catch (IOException e) {
            System.out.println("Erro ao salvar os tempos de execucao: " + e.getMessage());
        }
    }
}

class TarefaProcessamento implements Runnable {
    private List<File> arquivos;
    private DecimalFormat df = new DecimalFormat("#.##"); // Para formatar as saídas numéricas com duas casas decimais

    public TarefaProcessamento(List<File> arquivos) {
        this.arquivos = arquivos;
    }

    @Override
    public void run() {
        for (File arquivo : arquivos) {
            // Lógica para processar os dados do arquivo CSV
            processarArquivoCSV(arquivo);
        }
    }

    private void processarArquivoCSV(File arquivo) {
        // Mapa para armazenar as temperaturas organizadas por mês (1 a 12)
        Map<Integer, List<Double>> temperaturasPorMes = new HashMap<>();

        // Inicializar listas de temperaturas para cada mês
        for (int mes = 1; mes <= 12; mes++) {
            temperaturasPorMes.put(mes, new ArrayList<>());
        }

        try (BufferedReader br = new BufferedReader(new FileReader(arquivo))) {
            String linha;
            boolean primeiraLinha = true;  // Para ignorar o cabeçalho

            while ((linha = br.readLine()) != null) {
                // Ignorar o cabeçalho (primeira linha)
                if (primeiraLinha) {
                    primeiraLinha = false;
                    continue;
                }

                // Separar os valores por vírgula
                String[] valores = linha.split(",");

                // Certificar-se de que a linha tem pelo menos 6 colunas (conforme o formato esperado)
                if (valores.length < 6) {
                    System.out.println("Linha invalida no arquivo: " + arquivo.getName() + " -> " + linha);
                    continue;
                }

                try {
                    int mes = Integer.parseInt(valores[2]); // Coluna 3 (Month)
                    double temperatura = Double.parseDouble(valores[5]); // Coluna 6 (AvgTemperature)
                    // Adicionar a temperatura à lista correspondente ao mês
                    temperaturasPorMes.get(mes).add(temperatura);
                } catch (NumberFormatException e) {
                    System.out.println("Erro ao converter mes ou temperatura no arquivo: " + arquivo.getName() + " -> " + linha);
                }
            }

            // Calcular média, máxima e mínima para cada mês
            StringBuilder resultado = new StringBuilder();
            resultado.append("Arquivo: ").append(arquivo.getName()).append("\n");

            for (int mes = 1; mes <= 12; mes++) {
                List<Double> temperaturas = temperaturasPorMes.get(mes);

                if (!temperaturas.isEmpty()) {
                    double media = calcularMedia(temperaturas);
                    double maxima = Collections.max(temperaturas);
                    double minima = Collections.min(temperaturas);

                    // Formatar e imprimir os resultados por mês
                    resultado.append("Mes: ").append(mes).append("\n");
                    resultado.append("Temperatura Media: ").append(df.format(media)).append("\n");
                    resultado.append("Temperatura Maxima: ").append(df.format(maxima)).append("\n");
                    resultado.append("Temperatura Minima: ").append(df.format(minima)).append("\n");
                    resultado.append("------------------------------------\n");
                }
            }

            // Imprimir no console
            System.out.println(resultado);

        } catch (IOException e) {
            System.out.println("Erro ao ler o arquivo CSV: " + arquivo.getName());
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
