import { Collection, Doc, limit, query, Query } from 'typesaurus'
import PQueue from 'p-queue'

export type ProcessQueryGetQueries<Model> = (
  last: Doc<Model> | undefined
) => Query<Model, keyof Model>[]

export type ProcessQueryProcessor<Model> = (doc: Doc<Model>) => Promise<any>

export interface ProcessDocsOptions<Model> {
  queries: ProcessQueryGetQueries<Model>
  process: ProcessQueryProcessor<Model>
  pageSize?: number
  concurrency?: number
  timeout?: number
}

export async function processQuery<Model>(
  collection: Collection<Model>,
  {
    queries,
    process,
    pageSize = 100,
    concurrency = 10,
    timeout = Infinity
  }: ProcessDocsOptions<Model>
) {
  const startedAt = Date.now()
  const queue = new PQueue({ concurrency })

  async function processPage(after?: Doc<Model>) {
    const docs = await query(collection, queries(after).concat(limit(pageSize)))

    await Promise.all(
      docs.map((doc) =>
        queue.add(() => {
          if (Date.now() - startedAt < timeout) return process(doc)
        })
      )
    )

    if (docs.length === pageSize && Date.now() - startedAt < timeout)
      await processPage(docs[docs.length - 1]!)
  }

  return processPage()
}
