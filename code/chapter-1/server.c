#include <stdio.h> // For printf
#include <netdb.h> // For bind, listen, AF_INET, SOCK_STREAM, socklen_t, sockaddr_in, INADDR_ANY
#include <stdlib.h> // For exit
#include <string.h> // For bzero
#include <unistd.h> // For close & write
#include <errno.h> // For errno, duh!
#include <arpa/inet.h> // For inet_ntop

#define MAX 80
#define PORT 2000
#define SA struct sockaddr

int main()
{
    socklen_t client_address_length;
    int server_socket_file_descriptor, client_socket_file_descriptor;
    struct sockaddr_in server_address, client_address;

    // socket create and verification
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
    server_address.sin_addr.s_addr = htonl(INADDR_ANY);
    server_address.sin_port = htons(PORT);

    // Binding newly created socket to given IP and verification
    if ((bind(server_socket_file_descriptor, (SA*)&server_address, sizeof(server_address))) != 0) {
        printf("socket bind failed... : %d, %d\n", server_socket_file_descriptor, errno);
        exit(0);
    }
    else {
        printf("Socket successfully binded..\n");
    }

    // Now server is ready to listen and verification
    if ((listen(server_socket_file_descriptor, 5)) != 0) {
        printf("Listen failed...\n");
        exit(0);
    }
    else {
        printf("Server listening..\n");
    }
    client_address_length = sizeof(client_address);

    // Accept the data packet from client and verification
    client_socket_file_descriptor = accept(server_socket_file_descriptor, (SA*)&client_address, &client_address_length);
    if (client_socket_file_descriptor < 0) {
        printf("server acccept failed: %d,%d...\n", client_socket_file_descriptor, errno);
        exit(0);
    }
    else {
        printf("server acccept the client...\n");
        char human_readable_address[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_address.sin_addr, human_readable_address, sizeof(human_readable_address));
        printf("Client address: %s\n", human_readable_address);
    }

    char message_buffer[MAX];
    read(client_socket_file_descriptor, message_buffer, sizeof(message_buffer));
    printf("From Client: %s\n", message_buffer);
    bzero(message_buffer, MAX);

    strcpy(message_buffer, "Hello, this is Server!");
    write(client_socket_file_descriptor, message_buffer, sizeof(message_buffer));

    // After chatting close the socket
    printf("Closing server_socket_file_descriptor\n");
    close(server_socket_file_descriptor);
}
