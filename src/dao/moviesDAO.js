import { ObjectId } from "bson"

let movies
let mflix
const DEFAULT_SORT = [["tomatoes.viewer.numReviews", -1]]

export default class MoviesDAO {
  static async injectDB(conn) {
    if (movies) return
    try {
      mflix = await conn.db(process.env.MFLIX_NS)
      movies = await conn.db(process.env.MFLIX_NS).collection("movies")
      this.movies = movies // this is only for testing
    }
    catch (e) {
      console.error(
        `Unable to establish a collection handle in moviesDAO: ${e}`,
      )
    }
  }

  static async getConfiguration() {
    const roleInfo = await mflix.command({ connectionStatus: 1 })
    const authInfo = roleInfo.authInfo.authenticatedUserRoles[0]
    const { poolSize, wtimeout } = movies.s.db.serverConfig.s.options
    let response = {
      poolSize,
      wtimeout,
      authInfo,
    }
    return response
  }

  static async getMoviesByCountry(countries) {
    let cursor
    try {
      cursor = await movies.find({ countries: { $in: countries } }, { projection: { title: 1 } })
    }
    catch (e) {
      console.error(`Unable to issue find command, ${e}`)
      return []
    }
    return cursor.toArray()
  }

  static textSearchQuery(text) {
    const query = { $text: { $search: text } }
    const meta_score = { $meta: "textScore" }
    const sort = [["score", meta_score]]
    const project = { score: meta_score }

    return { query, project, sort }
  }

  static castSearchQuery(cast) {
    const searchCast = Array.isArray(cast) ? cast : cast.split(", ")
    const query = { cast: { $in: searchCast } }
    const project = {}
    const sort = DEFAULT_SORT

    return { query, project, sort }
  }

  static genreSearchQuery(genre) {
    const searchGenre = Array.isArray(genre) ? genre : genre.split(", ")
    const query = { genres: { $in: searchGenre } }
    const project = {}
    const sort = DEFAULT_SORT

    return { query, project, sort }
  }

  static async facetedSearch({
    filters = null, page = 0, moviesPerPage = 20, } = {}
  ) {
    if (!filters || !filters.cast) {
      throw new Error("Must specify cast members to filter by.")
    }
    const matchStage = { $match: filters }
    const sortStage = { $sort: { "tomatoes.viewer.numReviews": -1 } }
    const countingPipeline = [matchStage, sortStage, { $count: "count" }]
    const skipStage = { $skip: moviesPerPage * page }
    const limitStage = { $limit: moviesPerPage }
    const facetStage = {
      $facet: {
        runtime: [
          {
            $bucket: {
              groupBy: "$runtime",
              boundaries: [0, 60, 90, 120, 180],
              default: "other",
              output: {
                count: { $sum: 1 },
              },
            },
          },
        ],
        rating: [
          {
            $bucket: {
              groupBy: "$metacritic",
              boundaries: [0, 50, 70, 90, 100],
              default: "other",
              output: {
                count: { $sum: 1 },
              },
            },
          },
        ],
        movies: [
          {
            $addFields: {
              title: "$title",
            },
          },
        ],
      },
    }
    const queryPipeline = [
      matchStage,
      sortStage,
      skipStage,
      limitStage,
      facetStage,
    ]
    try {
      const results = await (await movies.aggregate(queryPipeline)).next()
      const count = await (await movies.aggregate(countingPipeline)).next()
      return { ...results, ...count }
    }
    catch (e) {
      return { error: "Results too large, be more restrictive in filter" }
    }
  }

  static async getMovies(
    { filters = null, page = 0, moviesPerPage = 20, } = {}
  ) {
    let queryParams = {}
    if (filters) {
      if ("text" in filters)
        queryParams = this.textSearchQuery(filters["text"])
      else if ("cast" in filters)
        queryParams = this.castSearchQuery(filters["cast"])
      else if ("genre" in filters)
        queryParams = this.genreSearchQuery(filters["genre"])
    }
    let { query = {}, project = {}, sort = DEFAULT_SORT } = queryParams
    let cursor

    try {
      cursor = await movies
        .find(query)
        .project(project)
        .sort(sort)
    }
    catch (e) {
      console.error(`Unable to issue find command, ${e}`)
      return { moviesList: [], totalNumMovies: 0 }
    }
    const displayCursor = cursor.skip(page * moviesPerPage).limit(moviesPerPage)

    try {
      const moviesList = await displayCursor.toArray()
      const totalNumMovies = page === 0 ? await movies.countDocuments(query) : 0
      return { moviesList, totalNumMovies }
    }
    catch (e) {
      console.error(`Unable to convert cursor to array or problem counting documents, ${e}`)
      return { moviesList: [], totalNumMovies: 0 }
    }
  }

  static async getMovieByID(id) {
    try {
      const pipeline = [
        {
          '$match': {
            _id: ObjectId(id)
          }
        }, {
          '$lookup': {
            'from': 'comments',
            'let': { 'id': '$_id' },
            'pipeline': [
              {
                '$match': {
                  '$expr': { '$eq': ['$movie_id', '$$id'] }
                }
              },
              { '$sort': { 'date': -1 } }
            ],
            'as': 'comments'
          }
        },

      ]
      return await movies.aggregate(pipeline).sort({ 'comments.date': -1 }).next()
    } 
    catch (e) {
      console.error(`Something went wrong in getMovieByID: ${e}`)
      return null;
    }
  }
}
