#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <cctype>
#include <fstream>
#include <sstream>
#include <algorithm>

#define DEBUG 1   
#define N_GRAM_SIZE 3      // Tamanho do n-grama
#define MIN_THRESHOLD 2    // Limiar mínimo para exibir
#define MAX_NGRAM_LEN 256  // Tamanho máximo de um n-grama

using namespace std;

// Lê arquivo para string
string read_file_to_string(const char *path) {
    ifstream file(path);
    if (!file.is_open()) {
        cerr << "Erro ao abrir arquivo: " << path << endl;
        return "";
    }
    stringstream buffer;
    buffer << file.rdbuf();
    return buffer.str();
}

// Tokeniza e normaliza o texto
vector<string> tokenize(const string& text) {
    vector<string> tokens;
    stringstream ss(text);
    string word;
    
    while (ss >> word) {
        string cleaned;
        for (char c : word) {
            if (isalpha(c)) {
                cleaned += tolower(c);
            }
        }
        if (!cleaned.empty()) {
            tokens.push_back(cleaned);
        }
    }
    return tokens;
}

// Gera n-gramas e conta ocorrências localmente
map<string, int> generateAndCountNgrams(const vector<string>& tokens, int N) {
    map<string, int> ngramCounts;
    
    if ((int)tokens.size() < N) return ngramCounts;
    
    for (size_t i = 0; i <= tokens.size() - N; i++) {
        string ngram = tokens[i];
        for (int j = 1; j < N; j++) {
            ngram += " " + tokens[i + j];
        }
        ngramCounts[ngram]++;
    }
    
    return ngramCounts;
}

// Mescla dois mapas de n-gramas
void mergeNgramMaps(map<string, int>& dest, const map<string, int>& src) {
    for (const auto& pair : src) {
        dest[pair.first] += pair.second;
    }
}

// Imprime n-gramas com filtro de limiar
void printNgrams(const map<string, int>& ngrams, int min_threshold) {
    int total_ngrams = 0;
    for (const auto& pair : ngrams) {
        total_ngrams += pair.second;
    }
    
    cout << "\n--- N-gramas Significativos (Limiar: " << min_threshold << ") ---\n";
    for (const auto& pair : ngrams) {
        if (pair.second >= min_threshold) {
            double relative_freq = (double)pair.second / total_ngrams;
            cout << "'" << pair.first << "' \t (Contagem: " << pair.second 
                 << ", Frequência: " << (relative_freq * 100.0) << "%)\n";
        }
    }
}
// Envia vetor de tokens via MPI
void sendTokens(const vector<string>& tokens, int dest) {
    int num_tokens = tokens.size();
    MPI_Send(&num_tokens, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
    
    for (const auto& token : tokens) {
        int len = token.length() + 1;
        MPI_Send(&len, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
        MPI_Send(token.c_str(), len, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
    }
}

// Recebe vetor de tokens via MPI
vector<string> receiveTokens(int source) {
    MPI_Status status;
    int num_tokens;
    MPI_Recv(&num_tokens, 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);
    
    vector<string> tokens;
    for (int i = 0; i < num_tokens; i++) {
        int len;
        MPI_Recv(&len, 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);
        char* buffer = new char[len];
        MPI_Recv(buffer, len, MPI_CHAR, source, 0, MPI_COMM_WORLD, &status);
        tokens.push_back(string(buffer));
        delete[] buffer;
    }
    return tokens;
}

// Envia mapa de n-gramas via MPI
void sendNgramMap(const map<string, int>& ngrams, int dest) {
    int num_ngrams = ngrams.size();
    MPI_Send(&num_ngrams, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
    
    for (const auto& pair : ngrams) {
        int len = pair.first.length() + 1;
        MPI_Send(&len, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
        MPI_Send(pair.first.c_str(), len, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
        MPI_Send(&pair.second, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
    }
}

// Recebe mapa de n-gramas via MPI
map<string, int> receiveNgramMap(int source) {
    MPI_Status status;
    int num_ngrams;
    MPI_Recv(&num_ngrams, 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);
    
    map<string, int> ngrams;
    for (int i = 0; i < num_ngrams; i++) {
        int len;
        MPI_Recv(&len, 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);
        char* buffer = new char[len];
        MPI_Recv(buffer, len, MPI_CHAR, source, 0, MPI_COMM_WORLD, &status);
        
        int count;
        MPI_Recv(&count, 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);
        
        ngrams[string(buffer)] = count;
        delete[] buffer;
    }
    return ngrams;
}

int ngram_parallel(int delta) {
    MPI_Init(NULL, NULL);

    int my_rank, nprocs;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    // Inicia contagem de tempo
    double start_time = MPI_Wtime();

    vector<string> tokens;
    int parent_rank = -1;

    if (my_rank != 0) {
        // Não sou a raiz, recebo tokens do pai
        parent_rank = (my_rank - 1) / 2;
        tokens = receiveTokens(parent_rank);
        
        #ifdef DEBUG
        cout << "[Rank " << my_rank << "] Recebi " << tokens.size() << " tokens do pai " << parent_rank << endl;
        #endif
    } else {
        // Sou raiz, leio e tokenizo o arquivo
        const char *input_path = "big_bible.txt";
        string text = read_file_to_string(input_path);
        if (text.empty()) {
            MPI_Finalize();
            return 1;
        }

        #ifdef DEBUG
        cout << "[Rank " << my_rank << "] Tokenizando texto..." << endl;
        #endif

        tokens = tokenize(text);

        #ifdef DEBUG
        cout << "[Rank " << my_rank << "] Total de tokens: " << tokens.size() << endl;
        #endif
    }

    int num_tokens = tokens.size();
    int left_child = (2 * my_rank) + 1;
    int right_child = (2 * my_rank) + 2;

    map<string, int> ngramCounts;

    // Decidir: dividir ou conquistar?
    if (num_tokens <= delta || left_child >= nprocs) {
        // Conquisto: gero n-gramas e conto
        #ifdef DEBUG
        cout << "[Rank " << my_rank << "] Conquistando com " << num_tokens << " tokens" << endl;
        #endif
        
        ngramCounts = generateAndCountNgrams(tokens, N_GRAM_SIZE);
    } else {
        // Divido: envio tokens para os filhos
        #ifdef DEBUG
        cout << "[Rank " << my_rank << "] Dividindo. Esq: " << left_child << " | Dir: " << right_child << endl;
        #endif

        int tokens_left = num_tokens / 2;
        int tokens_right = num_tokens - tokens_left;

        // Criar sub-vetores para cada filho
        vector<string> left_tokens(tokens.begin(), tokens.begin() + tokens_left);
        vector<string> right_tokens(tokens.begin() + tokens_left, tokens.end());

        // Envio para filho esquerdo
        sendTokens(left_tokens, left_child);

        // Envio para filho direito (se existir)
        if (right_child < nprocs) {
            sendTokens(right_tokens, right_child);
        } else {
            // Filho direito não existe, processo eu mesmo
            #ifdef DEBUG
            cout << "[Rank " << my_rank << "] Filho direito não existe, processando localmente" << endl;
            #endif
            ngramCounts = generateAndCountNgrams(right_tokens, N_GRAM_SIZE);
        }

        // Recebo resultados do filho esquerdo
        map<string, int> left_ngrams = receiveNgramMap(left_child);
        mergeNgramMaps(ngramCounts, left_ngrams);

        // Recebo resultados do filho direito (se existir)
        if (right_child < nprocs) {
            map<string, int> right_ngrams = receiveNgramMap(right_child);
            mergeNgramMaps(ngramCounts, right_ngrams);
        }

        #ifdef DEBUG
        cout << "[Rank " << my_rank << "] Mesclei resultados dos filhos" << endl;
        #endif
    }

    // Envio para o pai ou imprimo se sou raiz
    if (my_rank != 0) {
        #ifdef DEBUG
        cout << "[Rank " << my_rank << "] Enviando " << ngramCounts.size() << " n-gramas únicos para o pai" << endl;
        #endif
        sendNgramMap(ngramCounts, parent_rank);
    } else {
        // Sou a raiz, imprimo resultados
        double end_time = MPI_Wtime();
        double elapsed_time = end_time - start_time;
        
        printNgrams(ngramCounts, MIN_THRESHOLD);
        
        cout << "\n===========================================\n";
        cout << "Tempo total de execução: " << elapsed_time << " segundos\n";
        cout << "Número de processos: " << nprocs << endl;
        cout << "===========================================\n";
    }

    MPI_Finalize();
    return 0;
}

int main() {
    int delta = 1000; // Threshold para conquistar (número mínimo de tokens)
    ngram_parallel(delta);
    return 0;
}

