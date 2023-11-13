import {Pinecone,Vector ,PineconeRecord, PineconeClient, UpsertRequest, UpsertOperationRequest, utils as PineconeUtils} from "@pinecone-database/pinecone"
import { downloadFromS3 } from "./s3-server"
import {PDFLoader} from 'langchain/document_loaders/fs/pdf'
import {Document, RecursiveCharacterTextSplitter} from '@pinecone-database/doc-splitter'
import { getEmbeddings } from "./embeddings"
import { convertToAscii } from "./utils"
import md5 from 'md5'

const pinecone = new Pinecone({
  apiKey: process.env.PINECONE_API_KEY!,
  environment: process.env.PINECONE_ENVIRONMENT!
})

type PDFPage = {
  pageContent: string;
  metadata:{
    loc:{pageNumber:number}
  }
}

export async function loadS3IntoPinecone(fileKey: string) {
  //1. obtain the pdf -> download and read from pdf
  console.log("downloading s3 into file system")
  const file_name =  await downloadFromS3(fileKey)
  if(!file_name){
    throw new Error("could not download from s3")
  }
  const loader = new PDFLoader(file_name)
  const pages = (await loader.load()) as PDFPage[];

  // 2. split and segment the pdf into smaller pages
  const documents = await Promise.all(pages.map(page=>prepareDocument(page)))

  //3. vectorise and embed individual documents
  const vectors = await Promise.all(documents.flat().map(embedDocument))

  //4. upload to pinecone


  const pineconeIndex = pinecone.index('chatpdf-qjy')
  console.log('inserting vectors into pinecone')

  await pineconeIndex.upsert(vectors)
  return documents[0]

}

async function embedDocument(doc: Document){
  try {
    const embeddings = await getEmbeddings(doc.pageContent)
    const hash = md5(doc.pageContent)

    return {
      id: hash,
      values: embeddings,
      metadata: {
        text: doc.metadata.text,
        pageNumber: doc.metadata.pageNumber,
      } ,
    } as Vector

  } catch (error) {
    console.log("error embedding document", error)
    throw error
  }
}

export const truncateStringByByte = (str: string, bytes: number)=> {
  const enc = new TextEncoder()
  return new TextDecoder('utf-8').decode(enc.encode(str).slice(0, bytes))
}

async function prepareDocument(page: PDFPage){
  let {pageContent, metadata} = page
  pageContent.replace(/\n/g, ' ')
  //split the docs
  const splitter = new RecursiveCharacterTextSplitter()
  const docs = await splitter.splitDocuments([
    new Document({
      pageContent,
      metadata:{
        pageNumber:metadata.loc.pageNumber,
        text: truncateStringByByte(pageContent, 36000)
      }
    })
  ])
  return docs;
}