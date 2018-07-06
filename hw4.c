/*
 * hw4.c
 *
 *  Created on: May 19, 2018
 *      Author: gasha
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <fcntl.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

/// --------- DEFINES ---------
#define CHUNCK_SIZE 1024*1024
#define ERROR -1
#define SUCCESS 1

/// --------- STRUCTS ---------
typedef struct {
	int num_of_thread;
	char* read_file;
} ThreadInput;

typedef struct {
	int finished_curr_phase;
	int total_to_read;
	int curr_reads;
} HandleThread;

/// --------- GLOBAL VARIABLES ---------
int output_file = -1; // output file
pthread_mutex_t mutex;
pthread_cond_t* condition_arr = NULL;
char write_buffer[CHUNCK_SIZE];
HandleThread* handle_thread_arr = NULL;
int working_thread = 0;
int num_of_files = 0;
int len_written = 0;

/// --------- HELPER FUNCTIONS ---------

int calcLeftToRead(char* path_to_file) {
	//reference: https://stackoverflow.com/questions/238603/how-can-i-get-a-files-size-in-c
	float reads = 0;
	struct stat st;
	if (stat(path_to_file, &st) != 0) {
		printf("Error in fstat: %s\n", strerror(errno));
		exit(ERROR);
	}
	reads = (float) ((float) (st.st_size) / (float) (CHUNCK_SIZE));
	//reference: https://stackoverflow.com/questions/8377412/ceil-function-how-can-we-implement-it-ourselves
	int inum = (int) reads;
	if (reads == (float) inum) {
		return inum;
	}
	return inum + 1;

}

void initHandleArr(int i, char* file_name) {
	if (!file_name) {
		printf("Error in input file_name: %s\n", strerror(errno));
		exit(ERROR);
	}
	handle_thread_arr[i].total_to_read = calcLeftToRead(file_name);
	handle_thread_arr[i].curr_reads = 0;
	handle_thread_arr[i].finished_curr_phase = 0;
}

void xorOp(char* buffer_out, char* buffer_in, int len) {
	int i = 0;
	for (i = 0; i < len; i++) {
		buffer_out[i] = (char) buffer_out[i] ^ buffer_in[i];
	}
	if (len > len_written) {
		len_written = len;
	}
}

int nextThread() {
	int i = 0;
	for (i = 0; i < num_of_files; i++) {
		if ((handle_thread_arr[i].finished_curr_phase == 0)
				&& (handle_thread_arr[i].total_to_read
						> handle_thread_arr[i].curr_reads)) {
			return i;
		}
	}
	return -1;
}

int allFinished() {
	int i = 0;
	for (i = 0; i < num_of_files; i++) {
		if ((handle_thread_arr[i].total_to_read
				> handle_thread_arr[i].curr_reads)) {
			return 0;
		}
	}
	return 1;
}

void nullifyHandler() {
	int i = 0;
	for (i = 0; i < num_of_files; i++)
		handle_thread_arr[i].finished_curr_phase = 0;
}

int getOutputFileSize(char* file_name) {
	if (!file_name) {
		printf("Error in input file_name: %s\n", strerror(errno));
		exit(ERROR);
	}
	struct stat st;
	if (stat(file_name, &st) != 0) {
		printf("Error in fstat: %s\n", strerror(errno));
		exit(ERROR);
	}
	return (int) st.st_size;
}

void freeAndDestroyGlobal() {
	int j = 0;
	pthread_mutex_destroy(&mutex);
	for (j = 0; j < num_of_files; j++)
		pthread_cond_destroy(&condition_arr[j]);
	free(handle_thread_arr);
	free(condition_arr);
	close(output_file);
}

/// --------- THREAD FUNCTION ---------

void * handle_file(void* a_input) {
	char* read_buffer;
	int next_thread = -1;
	ThreadInput* l_input = (ThreadInput*) a_input;
	char* file = (char*) l_input->read_file;
	int thread_num = (int) l_input->num_of_thread;
	//open file to read
	int fd = open(file, O_RDONLY);
	if (fd < 0) {
		printf("Error opening thread file: %s\n", strerror(errno));
		exit(ERROR);
	}
	//create read_buffer
	read_buffer = malloc(CHUNCK_SIZE * sizeof(char));
	if (!read_buffer) {
		printf("Error allocating memory to read buffer: %s\n", strerror(errno));
		exit(ERROR);
	}

	//while there is something to read from the fd
	ssize_t bytes_read;
	while ((bytes_read = read(fd, read_buffer, CHUNCK_SIZE)) > 0) { // there is what to read!
		//lock mutex
		if (pthread_mutex_lock(&mutex) != 0) {
			printf("Error locking the mutex: %s\n", strerror(errno));
			exit(ERROR);
		}
		while (working_thread != thread_num) { // flag to avoid spontaneous awake
			if (pthread_cond_wait(&condition_arr[thread_num], &mutex) != 0) {
				printf("Error waiting: %s\n", strerror(errno));
				exit(ERROR);
			}
		}
		// finished waiting - do your job!
		xorOp(write_buffer, read_buffer, (int) bytes_read);
		//update handler
		handle_thread_arr[thread_num].curr_reads =
				handle_thread_arr[thread_num].curr_reads + 1;
		handle_thread_arr[thread_num].finished_curr_phase = 1;
		if ((next_thread = nextThread()) == -1) { // finished chunck in all threads
			if (write(output_file, write_buffer, len_written) == -1) { // write to file
				printf("Error writing to output file: %s\n", strerror(errno));
				exit(ERROR);
			}
			// nullify struct
			nullifyHandler();
			len_written = 0;
			memset(write_buffer, 0, CHUNCK_SIZE);
		}
		if (!allFinished()) {
			//signal the next thread
			next_thread = nextThread();
			working_thread = next_thread;
			if (pthread_cond_signal(&condition_arr[next_thread]) != 0) {
				printf("Error signal thread: %s\n", strerror(errno));
				exit(ERROR);
			}
		}
		//unlock mutex
		if (pthread_mutex_unlock(&mutex) != 0) {
			printf("Error unlock mutex: %s\n", strerror(errno));
			exit(ERROR);
		}
	}
	free((ThreadInput*) a_input);
        free(read_buffer);
	close(fd);
	pthread_exit(NULL);
}

/// --------- MAIN ---------
int main(int argc, char** argv) {
	int i = 0;
	int rc = 0;
	int output_size = 0;
	pthread_t *thread;
	pthread_attr_t attr;

	//check args
	if (argc < 2) {
		printf("Error in number of given args!\n");
		return ERROR;
	}
	num_of_files = argc - 2;
	printf("Hello, creating %s from %d input files\n", argv[1], num_of_files);

	//Initialize mutex and condition variable objects
	if (pthread_mutex_init(&mutex, NULL) != 0) {
		printf("Error initializing mutex: %s\n", strerror(errno));
		return ERROR;
	}
	//For portability, explicitly create threads in a joinable state
	if (pthread_attr_init(&attr) != 0) {
		printf("Error initializing pthread attr: %s\n", strerror(errno));
		return ERROR;
	}
	if (pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE) != 0) {
		printf("Error set detach state of pthread attr: %s\n", strerror(errno));
		return ERROR;
	}
	output_file = open(argv[1], O_WRONLY | O_TRUNC | O_CREAT, 00777);
	if (output_file < 0) {
		printf("Error opening output file: %s\n", strerror(errno));
		return ERROR;
	}
	// create array of threads
	thread = malloc(sizeof(pthread_t) * num_of_files);
	if (!thread) {
		printf("Error allocating memory to thread array: %s\n",
				strerror(errno));
		return ERROR;
	}
	// create and init cond array
	condition_arr = malloc(sizeof(pthread_cond_t) * num_of_files);
	if (!condition_arr) {
		printf("Error allocating memory to conditian array: %s\n",
				strerror(errno));
		return ERROR;
	}
	for (i = 0; i < num_of_files; i++) {
		if (pthread_cond_init(&condition_arr[i], NULL) != 0) {
			printf("Error initalizing pthread cond: %s\n", strerror(errno));
			return ERROR;
		}
	}

	//create array of HandleThread
	handle_thread_arr = malloc(num_of_files * sizeof(HandleThread));
	if (!handle_thread_arr) {
		printf("Error creating handle thread arr: %s\n", strerror(errno));
		return ERROR;
	}

	//fill HandleThread
	for (i = 0; i < num_of_files; i++) {
		initHandleArr(i, argv[i + 2]);
	}

	//init buffer
	memset(write_buffer, 0, CHUNCK_SIZE);

	//create threads
	for (i = 0; i < num_of_files; i++) {
		//create struct to be the input of the thread
		ThreadInput* thread_input = malloc(sizeof(ThreadInput));
		if (!thread_input) {
			printf("Error allocating memory to thread array: %s\n",
					strerror(errno));
			return ERROR;
		}
		//fill the struct
		thread_input->num_of_thread = i;
		thread_input->read_file = argv[i + 2];
		rc = pthread_create(&thread[i], &attr, handle_file,
				(void *) thread_input);
		if (rc != 0) {
			printf("Error creating thread: %s\n", strerror(errno));
			return ERROR;
		}
	}

	//join threads
	for (i = 0; i < num_of_files; i++) {
		if (pthread_join(thread[i], NULL) != 0) {
			printf("Error joining thread: %s\n", strerror(errno));
			return ERROR;
		}
	}

	//get output size
	output_size = getOutputFileSize(argv[1]);
	printf("Created %s with size %d bytes\n", argv[1], output_size);

	//exit gracefully
	pthread_attr_destroy(&attr);
	freeAndDestroyGlobal();
	free(thread);

	//bye!
	return SUCCESS;

}

