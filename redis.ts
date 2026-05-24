import { createClient } from "redis";

export const client = createClient({
    url: ""
});

client.on("error", (err) => {
    console.log("Redis error", err)
});

await client.connect();