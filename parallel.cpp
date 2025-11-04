#include <mpi.h>
#include <iostream>
#include <vector>
#include <string>
// map não é mais necessário se printNgrams for corrigido
#include <cctype>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <cstring>
#include <algorithm> // Para std::max

#define DEBUG 1   
#define N_GRAM_SIZE 5      // Tamanho do n-grama
#define MIN_THRESHOLD 2    // Limiar mínimo para exibir

// --- CONSTANTES DA NOVA LÓGICA ---
// Novo "delta", baseado em contagem de caracteres, não tokens.
// Se um nó recebe um bloco de texto menor que isso, ele conquista.
const size_t CHAR_THRESHOLD = 100000; // 100KB

// Overlap em caracteres. Deve ser grande o suficiente para
// capturar (N-1) tokens. 4KB é um palpite seguro.
const size_t OVERLAP_CHARS = 4096; 

using namespace std;

// --- Funções Auxiliares (Sem alteração) ---

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

vector<string> tokenize_optimized(const string& text) {
    vector<string> tokens;
    string current_word;
    tokens.reserve(text.length() / 5); 

    for (char c : text) {
        if (isalpha(c)) {
            current_word += tolower(c);
        } else if (!current_word.empty()) {
            tokens.push_back(current_word);
            current_word.clear();
        }
    }
    if (!current_word.empty()) {
        tokens.push_back(current_word);
    }
    return tokens;
}

// --- Funções de N-gram (Sem alteração) ---

unordered_map<string, int> generateAndCountNgrams(const vector<string>& tokens, int N, size_t start_index, size_t end_index) {
    unordered_map<string, int> ngramCounts;
    
    if (tokens.empty() || end_index <= start_index || end_index > tokens.size()) {
         return ngramCounts;
    }

    size_t last_start_index = (end_index >= (size_t)(N - 1)) ? end_index - (N - 1) : 0;
    if (last_start_index < start_index) return ngramCounts;

    for (size_t i = start_index; i <= last_start_index; i++) {
        if (i + N > tokens.size()) break; 
        string ngram = tokens[i];
        for (int j = 1; j < N; j++) {
            ngram += " " + tokens[i + j];
        }
        ngramCounts[ngram]++;
    }
    return ngramCounts;
}

void mergeNgramMaps(unordered_map<string, int>& dest, const unordered_map<string, int>& src) {
    for (const auto& pair : src) {
        dest[pair.first] += pair.second;
    }
}

// Corrigido para aceitar unordered_map
void printNgrams(const unordered_map<string, int>& ngrams, int min_threshold) {
    long long total_ngrams = 0; // Usar long long para contagens grandes
    for (const auto& pair : ngrams) {
        total_ngrams += pair.second;
    }
    
    cout << "\n--- N-gramas Significativos (Limiar: " << min_threshold << ") ---\n";
    for (const auto& pair : ngrams) {
        if (pair.second >= min_threshold) {
            double relative_freq = (total_ngrams > 0) ? (double)pair.second / total_ngrams : 0.0;
            cout << "'" << pair.first << "' \t (Contagem: " << pair.second 
                 << ", Frequência: " << (relative_freq * 100.0) << "%)\n";
        }
    }
}

// --- NOVAS Funções de Comunicação (Strings) ---

/**
 * Envia uma std::string de forma eficiente.
 * Formato: [size_t len] [char* data]
 */
void sendOptimizedString(const string& text, int dest) {
    size_t len = text.length();
    // Envia o tamanho
    MPI_Send(&len, 1, MPI_UNSIGNED_LONG, dest, 0, MPI_COMM_WORLD);
    // Envia os dados da string
    MPI_Send(text.c_str(), len, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
}

/**
 * Recebe uma std::string de forma eficiente.
 */
string receiveOptimizedString(int source) {
    MPI_Status status;
    size_t len;
    
    // Recebe o tamanho
    MPI_Recv(&len, 1, MPI_UNSIGNED_LONG, source, 0, MPI_COMM_WORLD, &status);
    
    // Aloca buffer ( +1 para o terminador nulo)
    char* buffer = new char[len + 1];
    
    // Recebe os dados
    MPI_Recv(buffer, len, MPI_CHAR, source, 0, MPI_COMM_WORLD, &status);
    
    // Adiciona terminador nulo e cria a string
    buffer[len] = '\0';
    string text(buffer);
    
    delete[] buffer;
    return text;
}


// --- Funções de Comunicação (Mapas) - Sem alteração ---

void sendOptimizedMap(const unordered_map<string, int>& ngrams, int dest) {
    int num_pairs = ngrams.size();
    if (num_pairs == 0) {
        size_t str_buffer_size = 0;
        MPI_Send(&num_pairs, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
        MPI_Send(&str_buffer_size, 1, MPI_UNSIGNED_LONG, dest, 0, MPI_COMM_WORLD);
        return;
    }
    
    size_t str_buffer_size = 0;
    for (const auto& pair : ngrams) {
        str_buffer_size += pair.first.length() + 1;
    }

    char* str_buffer = new char[str_buffer_size];
    int* counts_buffer = new int[num_pairs];
    
    size_t str_offset = 0;
    int count_idx = 0;
    for (const auto& pair : ngrams) {
        memcpy(str_buffer + str_offset, pair.first.c_str(), pair.first.length() + 1);
        str_offset += pair.first.length() + 1;
        counts_buffer[count_idx++] = pair.second;
    }

    MPI_Send(&num_pairs, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
    MPI_Send(&str_buffer_size, 1, MPI_UNSIGNED_LONG, dest, 0, MPI_COMM_WORLD);
    MPI_Send(str_buffer, str_buffer_size, MPI_CHAR, dest, 0, MPI_COMM_WORLD);
    MPI_Send(counts_buffer, num_pairs, MPI_INT, dest, 0, MPI_COMM_WORLD);
    
    delete[] str_buffer;
    delete[] counts_buffer;
}

unordered_map<string, int> receiveOptimizedMap(int source) {
    MPI_Status status;
    int num_pairs;
    size_t str_buffer_size;

    MPI_Recv(&num_pairs, 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);
    MPI_Recv(&str_buffer_size, 1, MPI_UNSIGNED_LONG, source, 0, MPI_COMM_WORLD, &status);

    if (num_pairs == 0) {
        return unordered_map<string, int>();
    }

    char* str_buffer = new char[str_buffer_size];
    int* counts_buffer = new int[num_pairs];
    
    MPI_Recv(str_buffer, str_buffer_size, MPI_CHAR, source, 0, MPI_COMM_WORLD, &status);
    MPI_Recv(counts_buffer, num_pairs, MPI_INT, source, 0, MPI_COMM_WORLD, &status);

    unordered_map<string, int> ngrams;
    size_t str_offset = 0;
    for (int i = 0; i < num_pairs; i++) {
        string ngram(str_buffer + str_offset);
        str_offset += ngram.length() + 1;
        ngrams[ngram] = counts_buffer[i];
    }
    
    delete[] str_buffer;
    delete[] counts_buffer;
    return ngrams;
}

// --- LÓGICA PRINCIPAL MODIFICADA ---

int ngram_parallel() {
    MPI_Init(NULL, NULL);

    int my_rank, nprocs;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);

    double start_time = MPI_Wtime();

    string local_text; // <-- O dado principal agora é a string de texto
    int parent_rank = (my_rank - 1) / 2;

    if (my_rank != 0) {
        // --- Processo Filho ---
        // 1. Recebe seu bloco principal de TEXTO
        string my_text_chunk = receiveOptimizedString(parent_rank);
        
        // 2. Recebe o bloco de TEXTO de SOBREPOSIÇÃO
        string overlap_text = receiveOptimizedString(parent_rank);
        
        // 3. Concatena para formar o texto local
        //    (Overlap vem primeiro)
        local_text = overlap_text + my_text_chunk;
        
        #if DEBUG
        cout << "[Rank " << my_rank << "] Recebi " << local_text.length() << " chars (incluindo " << overlap_text.length() << " de overlap) do pai " << parent_rank << endl;
        #endif

    } else {
        // --- Processo Raiz (Rank 0) ---
        const char *input_path = "big_bible.txt"; 
        string text = read_file_to_string(input_path);
        if (text.empty()) {
            MPI_Abort(MPI_COMM_WORLD, 1);
            return 1;
        }
        local_text = text; // O texto do Rank 0 é o arquivo inteiro

        #if DEBUG
        cout << "[Rank " << my_rank << "] Total de chars lidos: " << local_text.length() << endl;
        #endif
    }

    unordered_map<string, int> ngramCounts;
    int N = N_GRAM_SIZE;
    size_t num_local_chars = local_text.length();
    int left_child = (2 * my_rank) + 1;
    int right_child = (2 * my_rank) + 2;
    bool has_left_child = (left_child < nprocs);

    // Decidir: dividir ou conquistar?
    // Conquistar se o texto for pequeno OU se eu for uma folha na árvore MPI
    if (num_local_chars <= CHAR_THRESHOLD || !has_left_child) {
        // --- Conquistar ---
        #if DEBUG
        cout << "[Rank " << my_rank << "] Conquistando com " << num_local_chars << " chars" << endl;
        #endif
        
        // ** A TOKENIZAÇÃO ACONTECE AQUI, NO NÓ FOLHA **
        // Isso distribui o uso de memória do vector<string>
        vector<string> local_tokens = tokenize_optimized(local_text);
        
        ngramCounts = generateAndCountNgrams(local_tokens, N, 0, local_tokens.size());

    } else {
        // --- Dividir ---
        bool has_right_child = (right_child < nprocs);
        
        #if DEBUG
        cout << "[Rank " << my_rank << "] Dividindo " << num_local_chars << " chars. Esq: " << left_child << " | Dir: " << right_child << endl;
        #endif

        // 1. Encontrar ponto de divisão (no meio, em uma quebra de palavra)
        size_t split_index = num_local_chars / 2;
        size_t real_split = local_text.find_first_of(" \t\n", split_index);
        if (real_split == string::npos) {
            real_split = split_index; // Fallback se não achar espaço
        }

        // 2. Calcular overlap (baseado em CHARS)
        size_t overlap_start = (real_split > OVERLAP_CHARS) ? (real_split - OVERLAP_CHARS) : 0;
        
        // 3. Criar os pedaços de STRING
        string left_chunk_str = local_text.substr(0, real_split);
        string right_chunk_str = local_text.substr(real_split);
        // O overlap é [overlap_start ... real_split]
        string overlap_str = local_text.substr(overlap_start, real_split - overlap_start);

        // 4. Enviar para filho esquerdo
        sendOptimizedString(left_chunk_str, left_child);
        sendOptimizedString("", left_child); // Envia overlap vazio

        // 5. Enviar para filho direito (se existir) ou processar localmente
        if (has_right_child) {
            sendOptimizedString(right_chunk_str, right_child);
            sendOptimizedString(overlap_str, right_child); // Envia overlap real
        } else {
            // Filho direito não existe, processo o bloco direito eu mesmo
            #if DEBUG
            cout << "[Rank " << my_rank << "] Filho direito não existe, processando localmente" << endl;
            #endif
            
            // Concatena o texto
            string final_right_text = overlap_str + right_chunk_str;
            
            // ** A TOKENIZAÇÃO ACONTECE AQUI **
            vector<string> right_tokens = tokenize_optimized(final_right_text);
            
            // O resultado da direita vai direto para o `ngramCounts` deste nó
            ngramCounts = generateAndCountNgrams(right_tokens, N, 0, right_tokens.size());
        }

        // 6. Receber resultados do filho esquerdo
        unordered_map<string, int> left_ngrams = receiveOptimizedMap(left_child);
        mergeNgramMaps(ngramCounts, left_ngrams); // Mescla com os resultados da direita (se houver)

        // 7. Receber resultados do filho direito (se foi enviado)
        if (has_right_child) {
            unordered_map<string, int> right_ngrams = receiveOptimizedMap(right_child);
            mergeNgramMaps(ngramCounts, right_ngrams);
        }

        #if DEBUG
        cout << "[Rank " << my_rank << "] Mesclei resultados dos filhos" << endl;
        #endif
    }

    // --- Envio para o pai ou imprimo se sou raiz ---
    if (my_rank != 0) {
        #if DEBUG
        cout << "[Rank " << my_rank << "] Enviando " << ngramCounts.size() << " n-gramas únicos para o pai " << parent_rank << endl;
        #endif
        sendOptimizedMap(ngramCounts, parent_rank);
    } else {
        // Sou a raiz, imprimo resultados
        double end_time = MPI_Wtime();
        double elapsed_time = end_time - start_time;
        
        //printNgrams(ngramCounts, MIN_THRESHOLD); // Descomente se quiser ver os n-gramas
        
        cout << "\n===========================================\n";
        cout << "Tempo total de execução: " << elapsed_time << " segundos\n";
        cout << "Número de processos: " << nprocs << endl;
        cout << "Char Threshold: " << CHAR_THRESHOLD << endl;
        cout << "===========================================\n";
    }

    MPI_Finalize();
    return 0;
}


int main() {
    ngram_parallel();
    return 0;
}