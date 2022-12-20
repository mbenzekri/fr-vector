import fs from "fs"
import https from "https"
import http from "http"
import seven from "node-7z"
import { URL } from "url"


const ZIP_RE = /\.(zip|gz|gz2|7z)$/
export class DataProcess {

    constructor(conf) {
        this.conf = conf
    }
    log(message) {
        console.log(`${this.dataset.name} => ${message}`)
    }

    getFilename(response) {
        const contentDisposition = response.headers['content-disposition']
        if (contentDisposition == null) return null
        var regexp = /filename=\"(.*)\"/gi;
        const filename = regexp.exec(contentDisposition) [1]
        return filename
    }
    http(url) {
        if (url.startsWith("https://jarvis")) process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;

        if(/^http:/.test(url)) return http
        if(/^https:/.test(url)) return https
        throw `${this.dataset.name} => unknown protocol for url :  ${url}`
    }

    validUrl(url) {
        try {
            const u = new URL(url)
        } catch(e) {
            throw `Invalid url "${url}"`
        }
    }
    setDir(path) {
        if (fs.existsSync(path)) {
            try {
                const stats = fs.lstatSync(path);
                if (stats.isDirectory()) return
                throw `is already a File`
            } catch (e) {
                throw `fail to set directory '${path}' :  ${String(e)}`
            }
        }
        // directory doesn't exist create it
        try { fs.mkdirSync(path,{recursive: true})}
        catch(e) {
            throw `fail to create directory '${path}' :  ${String(e)}`
        }
    }

    uptodate(url,dir,filename = url.replace(/^.*\//,"")) {
        const target = `${dir}/${filename}`
        if (this.dataset.uptodate === "exist") return this.uptodateExists(target)
        if (this.dataset.uptodate === "head") return this.uptodateHead(url,target)
        throw `Unknown uptodate method for url "${url}" method="${method}" `
    }

    /**
     * test if target is an uptodate downloaded file for the provide url
     * @param {string} url 
     * @param {string} target 
     * @returns {boolean} true if target file is uptodate
     */
    async uptodateHead(url,target) {
        const http = this.http(url)
        return new Promise((resolve,reject) => {
            http.request(url, {method: "HEAD"}, (response) => {
                if (response.statusCode !== 200) {
                    reject(`${this.dataset.name} => Request Failed  ${response.statusCode} / ${response.statusText}`)
                }
                this.log(`DEBUG isUptoDate ${JSON.stringify(response.headers)}`)
                return true
            })
        })
    }

    /**
     * test if target is an existing file
     * @param {string} target 
     * @returns {boolean} true if target file exists
     */
    uptodateExists(target) {
        try { 
            return Promise.resolve(fs.existsSync(target))
        } catch(e) {
            Promise.reject(`Exist Fail on  ${target} error:  ${String(e)}`)
        }
    }

    /**
     * test if target is an existing file
     * @param {string} target 
     * @returns {boolean} true if target file exists
     */
    unzip(zip,target) {
        this.setDir(target)
        return new Promise((resolve,reject) => {
            // myStream is a Readable stream
            const myStream = seven.extractFull(zip, target, {
                $progress: true
            })
            
            myStream.on('data', (data) => {
                this.log(`Unzip status=${data.status} file=${data.file}`)
            })
            
            myStream.on('progress', (progress) => {
                this.log(`Unzip progress ${progress.percent} files=${progress.fileCount}`)
            })
            
            myStream.on('end', () => {
                // end of the operation, get the number of folders involved in the operation
                //myStream.info.get('Folders') //? '4'
                resolve()
            })
            
            myStream.on('error', (err) => reject(`Unzip Failed zip="${zip}" ${String(err)}`))              
        })
    }
    
    
    async updateDownload(url,dir,filename = url.replace(/^.*\//,"")) {
        this.setDir(dir)
        return new Promise(async (resolve,reject) => {
            const http = this.http(url)
            http.get(url, (response) => {
                if (response.statusCode !== 200) {
                    reject(`Request Failed  ${response.statusCode} / ${response.statusText}`)
                }
                filename = this.getFilename(response) ?? filename
                const target = `${dir}/${filename}`
                const writestream = fs.createWriteStream(target)
                writestream.on("finish", () => {
                    this.log(`Download GET "${url}" write Completed ${writestream.bytesWritten}`)
                    resolve(filename)
                })
                writestream.on("error", (e) => {
                    reject(`Download GET "${url}" write Error ${writestream.bytesWritten} error: ${String(e)}`)
                });
                response.pipe(writestream)
            }).on("error",(e) => {
                reject(`Download GET "${url}" error: ${String(e)}`)
            })
        })
    }
    async run() {
        for (const dataset of this.conf.datasets) {
            this.dataset = dataset
            try {
                this.log("------------------------------------------------ START")
                this.validUrl(dataset.url)
                const compressed = ZIP_RE.test(dataset.url)
                const zipdir = `${this.conf.workroot}/${this.dataset.name}/zip`
                const unzipdir = `${this.conf.workroot}/${this.dataset.name}/unzip`
                const dir = compressed ? zipdir : unzipdir
                const uptodate = await this.uptodate(dataset.url,dir)
                if (uptodate) { 
                    this.log(`Downloaded "${dataset.url}" is uptodate`)
                    this.log("------------------------------------------------ END")
                    continue
                }
                filename = await this.updateDownload(dataset.url,dir)
                const zipfile = `${zipdir}/${this.filename}`
                if (compressed && !uptodate) await this.unzip(zipfile,unzipdir)
                this.log("------------------------------------------------ END")

            } catch(message) {
                console.error(`${this.dataset.name} => ${message}`)
            }
        }
    }
}
