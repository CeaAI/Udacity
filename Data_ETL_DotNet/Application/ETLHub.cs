using System;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;

namespace Application
{
    public class ETLHub : Hub
    {
        public async Task ReceiveStream(IAsyncEnumerable<string> stream)
        {
            await foreach (var item in stream)
            {
                // deserialize string to json
                // carry out transformations

                // load to external json file  
            };

        }

        public async Task ReadSocketData(HttpContext httpContext, WebSocket webSocket)
        {
            var buffer = new byte[1024 * 4];
            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
            if(!result.CloseStatus.HasValue)
            {
                   // process result
                
            };
            
            await webSocket.CloseAsync(result.CloseStatus.Value, result.CloseStatusDescription, CancellationToken.None);

        }

        public async Task SendSocketData(WebSocket webSocket, ValueWebSocketReceiveResult result)
        {
            var buffer = new byte[1024 * 4];
            await webSocket.SendAsync(new ArraySegment<byte>(buffer, 0, result.Count), result.MessageType, result.EndOfMessage, CancellationToken.None);
        }
    }
}