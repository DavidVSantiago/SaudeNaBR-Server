async function run() {
    try {
        const line = `telemetria,idMotorista=123 bpm=80i,vfc=100i,spo2=98i ${Date.now()}\n`;
        const res = await fetch("http://127.0.0.1:9000/write?precision=ms", {
            method: "POST",
            body: line
        });
        console.log("Status:", res.status);
        console.log("Text:", await res.text());
    } catch (err) {
        console.error("Failed:", err);
    }
}
run();
