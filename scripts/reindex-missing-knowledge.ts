import { qdrantClient } from "../src/pkg/qdrant"
import { GlobalPubSub } from "../src/pkg/pubsub"
import { PUBSUB_TOPICS } from "../src/entities/PubSub"
import {
	Knowledge,
	KnowledgeAttachment,
	KnowledgeStatus,
	UserKnowledge,
	PrismaClient,
} from "../generated/prisma/client"

// Copy logic exactly as how KnowledgeService generates the message
function generateContentKnowledge(knowledge: any) {
	return `
        Headline: ${knowledge.headline}
        Category: ${knowledge.category}
        Sub Category: ${knowledge.subCategory}
        Case: ${knowledge.case}
        Knowledge Content: 
            ${knowledge.knowledgeContent
							.map(
								(content: any) =>
									`Title: ${content.title}, Description: ${content.description}`,
							)
							.join("\n")}
    `
}

function generateKnowledgeQueueDTO(
	knowledge: Knowledge & { userKnowledge: UserKnowledge[] } & {
		knowledgeAttachment: KnowledgeAttachment[]
	},
) {
	return {
		metadata: {
			knowledgeId: knowledge.id,
			type: knowledge.type,
			access: knowledge.access,
			headline: knowledge.headline,
			tenantId: knowledge.tenantId,
			accessUserIds:
				knowledge.access == "EMAIL"
					? Array.from(
							new Set([
								...knowledge.userKnowledge.map(
									(userKnowledge: any) => userKnowledge.user.id,
								),
								knowledge.createdByUserId,
							]),
						)
					: [],
		},
		fileType:
			knowledge.knowledgeAttachment && knowledge.knowledgeAttachment.length > 0
				? (() => {
						const extensions = knowledge.knowledgeAttachment.map(
							(attachment: any) =>
								attachment.attachmentUrl.split(".").pop()?.toLowerCase(),
						)
						if (
							extensions.some((ext: any) =>
								["xlsx", "xls", "csv"].includes(ext),
							)
						) {
							return "SPREADSHEET"
						}
						if (extensions.includes("pdf") || extensions.includes("docx")) {
							return "PDF"
						}
						return "IMAGE"
					})()
				: "",
		fileUrls: knowledge.knowledgeAttachment.map(
			(attachment: any) => attachment.attachmentUrl,
		),
		content: generateContentKnowledge(knowledge),
	}
}

async function runReindex() {
	console.log("🚀 Starting reindex script for missing Qdrant documents...")
	const prisma = new PrismaClient()

	try {
		console.log("Fetching all APPROVED knowledge from Postgres...")
		const approvedKnowledge = await prisma.knowledge.findMany({
			where: {
				status: KnowledgeStatus.APPROVED,
                isArchived: false,
			},
			include: {
				knowledgeAttachment: true,
				knowledgeContent: true,
				userKnowledge: {
					include: {
						user: true,
					},
				},
			},
		})

		console.log(`Found ${approvedKnowledge.length} APPROVED non-archived knowledge articles in Postgres.`)

		console.log("Fetching all indexed knowledge from Qdrant...")
		const existingQdrantIds = new Set<string>()
		let offset: string | null = null

		do {
			const scrollResponse = await qdrantClient.scroll("radikari_knowledge", {
				limit: 100,
				offset: offset ?? undefined,
				with_payload: true,
				with_vector: false,
			})

			for (const point of scrollResponse.points) {
				const payload = point.payload as any
				if (payload && payload.knowledge_id) {
					existingQdrantIds.add(payload.knowledge_id)
				}
			}

			offset = scrollResponse.next_page_offset as string | null
		} while (offset !== null)

		console.log(`Found ${existingQdrantIds.size} unique knowledge articles in Qdrant.`)

		const missingKnowledge = approvedKnowledge.filter(
			(k) => !existingQdrantIds.has(k.id),
		)

		console.log(`Identified ${missingKnowledge.length} missing knowledge articles that need indexing.`)

		if (missingKnowledge.length === 0) {
			console.log("✅ All APPROVED knowledge articles are already indexed in Qdrant. Nothing to do.")
			return
		}

		console.log("🔄 Connecting to RabbitMQ...")
		const pubsub = GlobalPubSub.getInstance().getPubSub()
		await pubsub.connect()
		console.log("✅ Custom RabbitMQ Connection Ready")

		console.log("📤 Pushing missing knowledge articles to KNOWLEDGE_CREATE queue...")
		let publishedCount = 0

		for (const knowledge of missingKnowledge) {
            // @ts-ignore
			const payload = generateKnowledgeQueueDTO(knowledge)
			
			console.log(`Publishing indexing task for ID: ${knowledge.id} - ${knowledge.headline}`)
			await pubsub.sendToQueue(PUBSUB_TOPICS.KNOWLEDGE_CREATE, payload)
			publishedCount++
            
            // tiny sleep to not flood connection
            await new Promise(r => setTimeout(r, 50))
		}

		console.log(`✅ Successfully published ${publishedCount} tasks to RabbitMQ.`)

		// Allow queue some time to properly empty buffer before disconnecting
		await new Promise(r => setTimeout(r, 2000))
        await pubsub.disconnect()
        console.log("👋 Disconnected RabbitMQ")

	} catch (error) {
		console.error("❌ Fatal error in reindex script:", error)
		throw error
	} finally {
		if (prisma) {
			await prisma.$disconnect()
			console.log("👋 Database disconnected")
		}
	}
}

// Keep process alive briefly
process.stdin.resume()

runReindex()
	.then(() => {
		console.log("✅ REINDEX SCRIPT COMPLETED")
		process.exit(0)
	})
	.catch((error) => {
		console.error("❌ Reindex failed:", error)
		process.exit(1)
	})
