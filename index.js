


import fs from 'node:fs'
const readStream = fs.createReadStream('./source.txt')
const writeStream = fs.createWriteStream('./dest.txt')

// 1- Use a readable stream to read a file in chunks and log each chunk. 
//• Input Example: "./big.txt"
//• Output Example: log each chunk

const read_stream = fs.createReadStream('./big.txt', { encoding: 'utf-8' })

readStream.on('data', (chunk) => {
    console.log('Chunk received:')
    console.log(chunk)
    console.log('---')
})

readStream.on('end', () => {
    console.log('Finished reading file')
})


//2-Use readable and writable streams to copy content from one file to another. 
//• Input Example: "./source.txt", "./dest.txt"
//• Output Example: File copied using streams





readStream.pipe(writeStream)

writeStream.on('finish', () => {
    console.log('File copied using streams')
})


//3-Create a pipeline that reads a file, compresses it, and writes it to another file.

import zlib from 'node:zlib'
import { pipeline } from 'node:stream'

pipeline(
    fs.createReadStream('./data.txt'),
    zlib.createGzip(),
    fs.createWriteStream('./data.txt.gz'),
    (err) => {
        if (err) {
            console.error('Pipeline failed:', err)
        } else {
            console.log('File compressed successfully')
        }
    }
)




// All CRUD operations using http

import http from 'http' 

const port = 3000
const readUsers = async ()=>{
    const data = await fs.readFile('./data.json' , 'utf-8')
    return JSON.parse(data);
}
const writeUsers = async (data) =>{
     await fs.writeFile ("./data.json" , JSON.stringify(data))
} 
const server = http.createServer ( async (req,res)=>{
 const {method , url} = req
 if(url=='/users' && method == 'GET'){
    const data = await readUsers()
    res.write(JSON.stringify(data))
    res.end()
 } else if(url=='/users' && method=="POST"){
    let data = ""
    req.on("data" , (chunk) =>{
        data+=chunk   
    })
    req.on("end" , async () => {
        data = JSON.parse(data)
        const {email , name} = data
        const users = await readUsers()
        const index = users.findIndex((ele) => ele.email == email)
        if (index != -1){
            res.write ("Email already exists")
            return res.end()
        }
        const lastId = users[users.length-1].id
        const newUser = {
            id : lastId +1 ,
            name ,
            email
        }
        users.push(newUser)
        await writeUsers(users)
        res.write("User added successfully")
        res.end()
    })
 } else if(url == '/users' && method == "PATCH"){
    let data = ""
    req.on("data" , (chunk)=>{
        data += chunk
    })
    req.on("end" , async ()=>{
        data=JSON.parse(data)
        const users = await readUsers()
        const {userId , name , email} = data
        const index = users.findIndex((ele)=> ele.id == userId)
        if(index == -1){
            res.write("User not found")
            return res.end()
        }
        if (email){
            const emailExists = users.findIndex((ele)=> ele.email == email)
            if (emailExists != -1 && email != users[index].email) {
                res.write ("Email already exists")
                return res.end()
            }
            users[index].email = email
        }
        if (name){
            users[index].name = name
        }
        await writeUsers(users)
        res.write ("User updated successfully")
        res.end()
    })
 } else if (url == "/users" && method == "DELETE"){
    let data = ""
    req.on("data" , (chunk)=>{
        data += chunk
    })
    req.on("end" ,async ()=>{
        data = JSON.parse(data)
        const users = await readUsers()
        const {userId}= data
        const index = users.findIndex((ele)=> ele.id == userId)
        if(index == -1){
            res.write ("User not found")
            return res.end()
        }
        users.splice(index,1)
        await writeUsers(users)
        res.write("user deleted successfully")
        res.end()
    })
 }
})
server.listen (3000,()=>{
    console.log("server is running on port 3000")
})

//______________________________________________________________________________//

//1- The event loop is the mechanism that lets Node.js — a single-threaded 
// JavaScript runtime — handle many concurrent operations (file I/O,
//  network requests, timers) without blocking.


//2- Libuv is a C library that Node.js is built on top of. It provides 
// the actual event loop implementation, plus an abstraction layer over 
// async I/O across different operating systems 
// (epoll on Linux, kqueue on macOS, IOCP on Windows).



//3- When you call an async function (e.g., fs.readFile), Node hands 
// the actual work off to libuv. Depending on the operation:
//Network I/O (sockets, most networking) is handled by the OS's native async 
// mechanisms (epoll/kqueue/IOCP) — no extra thread needed.
//File system operations, DNS lookups, and some CPU-bound built-ins 
// (like certain crypto functions) don't have a good async OS primitive, 
// so libuv runs them on its internal thread pool instead.



//4- Call stack: where JavaScript actually executes, synchronously, 
// one frame at a time. Function calls push frames on, returns pop them off. 
// JS is single-threaded, so there's exactly one call stack, and it must be e
// mpty before queued callbacks can run.
//Event queue (more precisely, several queues — callback queue, microtask queue,
//  etc.): holds callbacks that are ready to run once triggered by something 
// completing (a timer firing, an I/O operation finishing, a Promise resolving).
//  They sit here waiting their turn.
//Event loop: the orchestrator. 
// It continuously checks whether the call stack is empty, and if so, 
// pulls the next callback from the appropriate queue and pushes it onto the call
//  stack to execute. It's the mechanism that moves work from the queues onto the
//  stack, in a defined order.



//5- the pool size is 4 threads. 
//process.env.UV_THREADPOOL_SIZE = n


//6- Blocking code runs synchronously on the main thread and prevents anything else 
// — including other requests, timers, or I/O callbacks — from running until it 
// finishes. Example: fs.readFileSync(), a heavy synchronous loop, or 
// JSON.parse() on a huge string. While this runs, the event loop is stuck; 
// nothing else in your app can proceed.
//Non-blocking code hands the work off (to the OS's async I/O or libuv's 
// thread pool) and returns control to the main thread immediately. 
// The main thread continues executing other code, and the result comes back later
//  via a callback, Promise, or async/await, to be run when the event loop reaches
//  it. Example: fs.readFile(), any database query, any network request.