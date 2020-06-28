#include <stdio.h> // For printf
#include <netdb.h> // For AF_INET, SOCK_STREAM, sockaddr_in
#include <stdlib.h> // For exit
#include <string.h> // For bzero
#include <sys/socket.h> // For connect
#include <arpa/inet.h> // For inet_addr
#include <unistd.h> // for close

#define MAX 80
#define PORT 2000
#define SA struct sockaddr

int main() {
    int server_socket_file_descriptor;
    struct sockaddr_in server_address;

    // socket create and varification
    server_socket_file_descriptor = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket_file_descriptor == -1) {
        printf("socket creation failed...\n");
        exit(0);
    }
    else {
        printf("Socket successfully created..\n");
    }

    bzero(&server_address, sizeof(server_address));

    // assign IP, PORT
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = inet_addr("127.0.0.1");
    server_address.sin_port = htons(PORT);

    // connect the client socket to server socket
    if (connect(server_socket_file_descriptor, (SA*)&server_address, sizeof(server_address)) != 0) {
        printf("connection with the server failed...\n");
        exit(0);
    }
    else {
        printf("connected to the server..\n");
    }

    char message_buffer[MAX] = "Hello, this is Client";
    write(server_socket_file_descriptor, message_buffer, sizeof(message_buffer));
    bzero(message_buffer, sizeof(message_buffer));
    read(server_socket_file_descriptor, message_buffer, sizeof(message_buffer));
    printf("From Server: %s", message_buffer);

    // close the socket
    close(server_socket_file_descriptor);
}
