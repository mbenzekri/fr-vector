import conf from "./conf.json"
import { DataProcess } from "./process.js"
const job = new DataProcess(conf)
job.run()
