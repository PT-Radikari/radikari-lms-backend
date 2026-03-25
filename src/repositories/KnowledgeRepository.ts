import * as EzFilter from "@nodewave/prisma-ezfilter"
import { prisma } from "$pkg/prisma"
import {
	KnowledgeAttachmentDTO,
	KnowledgeContentDTO,
	KnowledgeDTO,
} from "$entities/Knowledge"
import {
	Knowledge,
	KnowledgeAccess,
	KnowledgeActivityLogAction,
	KnowledgeStatus,
	Prisma,
} from "../../generated/prisma/client"
import { ulid } from "ulid"
// import * as TenantRoleHelpers from "./helpers/TenantRoleHelpers"
import { UserJWTDAO } from "$entities/User"

export async function create(
	userId: string,
	tenantId: string,
	data: KnowledgeDTO,
) {
	return await prisma.$transaction(async (tx) => {
		const { attachments, contents, emails, ...rest } = data
		let knowledge: Knowledge

		// Access control is handled at query level, not by nullifying tenantId
		knowledge = await tx.knowledge.create({
			data: {
				...rest,
				tenantId, // Always store the tenant context
				createdByUserId: userId,
			},
		})

		//Create knowledge attachments
		await createAttachments(tx, knowledge.id, attachments)
		// Create knowledge contents and their attachments
		await createContent(tx, knowledge.id, contents)

		if (rest.access === KnowledgeAccess.EMAIL) {
			if (emails && emails.length > 0) {
				await createEmails(tx, knowledge.id, emails)
			}
		}

		// Create activity log
		await tx.knowledgeActivityLog.create({
			data: {
				id: ulid(),
				knowledgeId: knowledge.id,
				action: KnowledgeActivityLogAction.CREATE,
				createdByUserId: userId,
			},
		})

		return knowledge
	})
}

export async function createMany(data: Prisma.KnowledgeCreateManyInput[]) {
	return await prisma.knowledge.createMany({
		data: data,
	})
}

export async function createManyAttachments(
	data: Prisma.KnowledgeAttachmentCreateManyInput[],
) {
	return await prisma.knowledgeAttachment.createMany({
		data: data,
	})
}

export async function createManyContent(
	data: Prisma.KnowledgeContentCreateManyInput[],
) {
	return await prisma.knowledgeContent.createMany({
		data: data,
	})
}

export async function getAll(
	user: UserJWTDAO,
	tenantId: string,
	filters: EzFilter.FilteringQuery,
) {
	const queryBuilder = new EzFilter.BuildQueryFilter()
	let { filters: rawFilters, ...rest } = filters
	const usedFilters = queryBuilder.build(rest as any)
	// usedFilters = await TenantRoleHelpers.buildFilterTenantRole(usedFilters, user, tenantId)

	usedFilters.query.include = {
		createdByUser: {
			select: {
				id: true,
				fullName: true,
			},
		},
		parent: {
			include: {
				createdByUser: {
					select: {
						id: true,
						fullName: true,
					},
				},
			},
		},
	}

	// ✅ FIX: Parse filters if it's a string (JSON)
	if (typeof rawFilters === "string") {
		try {
			// eslint-disable-next-line @typescript-eslint/ban-ts-comment
			// @ts-ignore
			rawFilters = JSON.parse(rawFilters)
		} catch (e) {
			console.error("Failed to parse filters:", e)
			// eslint-disable-next-line @typescript-eslint/ban-ts-comment
			// @ts-ignore
			rawFilters = {}
		}
	}

	if (
		rawFilters &&
		typeof rawFilters === "object" &&
		Object.keys(rawFilters).length > 0
	) {
		const processedFilters: any = {}
		const andConditions: any[] = []

		for (const [key, value] of Object.entries(rawFilters)) {
			if (key === "subCategory" && Array.isArray(value) && value.length > 0) {
				// Handle SubCategory as CSV strings: OR [ { contains: val1 }, { contains: val2 } ]
				andConditions.push({
					OR: value.map((v) => ({
						subCategory: {
							contains: v,
						},
					})),
				})
			} else if (Array.isArray(value)) {
				processedFilters[key] = { in: value }
			} else {
				processedFilters[key] = value
			}
		}

		if (!usedFilters.query.where.AND) {
			usedFilters.query.where.AND = []
		}

		if (Object.keys(processedFilters).length > 0) {
			usedFilters.query.where.AND.push(processedFilters)
		}

		if (andConditions.length > 0) {
			usedFilters.query.where.AND.push(...andConditions)
		}
	}

	// Show all versions (remove the children filter)
	usedFilters.query.where.AND.push({
		isArchived: false,
	})

	usedFilters.query.where.AND.push({
		OR: [
			{
				access: {
					in: [KnowledgeAccess.PUBLIC, KnowledgeAccess.TENANT],
				},
				tenantId,
			},
			{
				access: KnowledgeAccess.EMAIL,
				userKnowledge: {
					some: {
						userId: user.id,
					},
				},
			},
		],
	})

	const [knowledge, totalData] = await Promise.all([
		prisma.knowledge.findMany(usedFilters.query as any),
		prisma.knowledge.count({
			where: usedFilters.query.where,
		}),
	])

	let totalPage = 1
	if (totalData > usedFilters.query.take)
		totalPage = Math.ceil(totalData / usedFilters.query.take)

	return {
		entries: knowledge,
		totalData,
		totalPage,
	}
}
export async function getAllArchived(
	user: UserJWTDAO,
	tenantId: string,
	filters: EzFilter.FilteringQuery,
) {
	const queryBuilder = new EzFilter.BuildQueryFilter()
	let { filters: rawFilters, ...rest } = filters
	const usedFilters = queryBuilder.build(rest as any)
	// usedFilters = await TenantRoleHelpers.buildFilterTenantRole(usedFilters, user, tenantId)

	usedFilters.query.include = {
		createdByUser: {
			select: {
				id: true,
				fullName: true,
			},
		},
	}

	// ✅ FIX: Parse filters if it's a string (JSON)
	if (typeof rawFilters === "string") {
		try {
			// eslint-disable-next-line @typescript-eslint/ban-ts-comment
			// @ts-ignore
			rawFilters = JSON.parse(rawFilters)
		} catch (e) {
			console.error("Failed to parse filters:", e)
			// eslint-disable-next-line @typescript-eslint/ban-ts-comment
			// @ts-ignore
			rawFilters = {}
		}
	}

	if (
		rawFilters &&
		typeof rawFilters === "object" &&
		Object.keys(rawFilters).length > 0
	) {
		const processedFilters: any = {}
		const andConditions: any[] = []

		for (const [key, value] of Object.entries(rawFilters)) {
			if (key === "subCategory" && Array.isArray(value) && value.length > 0) {
				// Handle SubCategory as CSV strings: OR [ { contains: val1 }, { contains: val2 } ]
				andConditions.push({
					OR: value.map((v) => ({
						subCategory: {
							contains: v,
						},
					})),
				})
			} else if (Array.isArray(value)) {
				processedFilters[key] = { in: value }
			} else {
				processedFilters[key] = value
			}
		}

		if (!usedFilters.query.where.AND) {
			usedFilters.query.where.AND = []
		}

		if (Object.keys(processedFilters).length > 0) {
			usedFilters.query.where.AND.push(processedFilters)
		}

		if (andConditions.length > 0) {
			usedFilters.query.where.AND.push(...andConditions)
		}
	}

	usedFilters.query.where.AND.push({
		isArchived: true,
	})

	usedFilters.query.where.AND.push({
		OR: [
			{
				access: {
					in: [KnowledgeAccess.PUBLIC, KnowledgeAccess.TENANT],
				},
				tenantId,
			},
			{
				access: KnowledgeAccess.EMAIL,
				userKnowledge: {
					some: {
						userId: user.id,
					},
				},
			},
		],
	})

	const [knowledge, totalData] = await Promise.all([
		prisma.knowledge.findMany(usedFilters.query as any),
		prisma.knowledge.count({
			where: usedFilters.query.where,
		}),
	])

	let totalPage = 1
	if (totalData > usedFilters.query.take)
		totalPage = Math.ceil(totalData / usedFilters.query.take)

	return {
		entries: knowledge,
		totalData,
		totalPage,
	}
}

export async function getSummary(
	user: UserJWTDAO,
	tenantId: string,
	filters: EzFilter.FilteringQuery,
) {
	const queryBuilder = new EzFilter.BuildQueryFilter()
	let { filters: rawFilters, ...rest } = filters

	// Parse filters if it's a string
	if (typeof rawFilters === "string") {
		try {
			rawFilters = JSON.parse(rawFilters)
		} catch (e) {
			console.error("Failed to parse filters:", e)
			rawFilters = {}
		}
	}

	const usedFilters = queryBuilder.build(rest as any)
	// usedFilters = await TenantRoleHelpers.buildFilterTenantRole(usedFilters, user, tenantId)

	usedFilters.query.include = {
		createdByUser: {
			select: {
				id: true,
				fullName: true,
			},
		},
	}

	usedFilters.query.where.AND.push({
		tenantId, // Filter by tenant first
	})

	usedFilters.query.where.AND.push({
		OR: [
			{
				access: KnowledgeAccess.PUBLIC,
			},
			{
				access: KnowledgeAccess.TENANT,
			},
			{
				access: KnowledgeAccess.EMAIL,
				userKnowledge: {
					some: {
						userId: user.id,
					},
				},
			},
			{
				createdByUserId: user.id,
			},
		],
	})

	const [pending, approved, revision, rejected] = await Promise.all([
		prisma.knowledge.findMany({
			where: {
				status: KnowledgeStatus.PENDING,
				...usedFilters.query.where,
			},
		}),
		prisma.knowledge.findMany({
			where: {
				status: KnowledgeStatus.APPROVED,
				...usedFilters.query.where,
			},
		}),
		prisma.knowledge.findMany({
			where: {
				status: KnowledgeStatus.REVISION,
				...usedFilters.query.where,
			},
		}),
		prisma.knowledge.findMany({
			where: {
				status: KnowledgeStatus.REJECTED,
				...usedFilters.query.where,
			},
		}),
	])

	return {
		pending: pending.length,
		approved: approved.length,
		revision: revision.length,
		rejected: rejected.length,
	}
}

export async function getById(id: string) {
	return await prisma.knowledge.findUnique({
		where: {
			id,
		},
		relationLoadStrategy: "join",
		select: {
			id: true,
			tenantId: true,
			createdByUserId: true,
			headline: true, // Explicitly select headline
			category: true,
			subCategory: true,
			case: true,
			access: true,
			type: true,
			status: true,
			createdAt: true,
			updatedAt: true,
			isArchived: true,
			version: true,
			parentId: true,
			rejectionComment: true,
			isDownloadHidden: true,
			userKnowledge: {
				select: {
					user: {
						select: {
							id: true,
							fullName: true,
						},
					},
				},
			},
			knowledgeAttachment: true,
			knowledgeContent: {
				select: {
					id: true,
					title: true,
					description: true,
					order: true,
					knowledgeContentAttachment: {
						orderBy: {
							order: "asc",
						},
					},
				},
				orderBy: {
					order: "asc",
				},
			},
			knowledgeActivityLog: {
				select: {
					id: true,
					action: true,
					createdAt: true,
					createdByUser: {
						select: {
							id: true,
							fullName: true,
							profilePictureUrl: true,
							role: true,
							tenantUser: {
								select: {
									tenantId: true,
									tenantRole: {
										select: {
											name: true,
										},
									},
								},
							},
						},
					},
				},
				orderBy: {
					createdAt: "desc",
				},
			},
			createdByUser: {
				select: {
					id: true,
					fullName: true,
				},
			},
		},
	})
}

export async function getByIds(ids: string[]) {
	return await prisma.knowledge.findMany({
		where: {
			id: {
				in: ids,
			},
		},
		relationLoadStrategy: "join",
		select: {
			id: true,
			tenantId: true,
			createdByUserId: true,
			headline: true,
			category: true,
			subCategory: true,
			case: true,
			access: true,
			type: true,
			status: true,
			createdAt: true,
			updatedAt: true,
			isArchived: true,
			version: true,
			parentId: true,
			rejectionComment: true,
			isDownloadHidden: true,
			userKnowledge: {
				select: {
					user: {
						select: {
							id: true,
							fullName: true,
						},
					},
				},
			},
			knowledgeAttachment: true,
			knowledgeContent: {
				select: {
					id: true,
					title: true,
					description: true,
					order: true,
					knowledgeContentAttachment: {
						orderBy: {
							order: "asc",
						},
					},
				},
				orderBy: {
					order: "asc",
				},
			},
			knowledgeActivityLog: {
				select: {
					id: true,
					action: true,
					createdAt: true,
					createdByUser: {
						select: {
							id: true,
							fullName: true,
							profilePictureUrl: true,
							tenantUser: {
								select: {
									tenantId: true,
									tenantRole: {
										select: {
											name: true,
										},
									},
								},
							},
						},
					},
				},
				orderBy: {
					createdAt: "desc",
				},
			},
			createdByUser: {
				select: {
					id: true,
					fullName: true,
				},
			},
		},
	})
}

export async function update(
	id: string,
	data: KnowledgeDTO,
	tenantId: string,
	status: KnowledgeStatus,
) {
	return await prisma.$transaction(async (tx) => {
		const { attachments, contents, emails, ...rest } = data
		let knowledge: Knowledge

		// FIXED: Always maintain tenantId to preserve context for AI service
		// Access control is handled at query level, not by nullifying tenantId
		knowledge = await tx.knowledge.update({
			where: { id },
			data: { ...rest, tenantId, status }, // Always maintain the tenant context
		})

		// Delete old attachments and contents (cascade will delete content attachments)
		await tx.knowledgeAttachment.deleteMany({
			where: { knowledgeId: id },
		})
		await tx.knowledgeContent.deleteMany({
			where: { knowledgeId: id },
		})

		// Recreate knowledge attachments
		await createAttachments(tx, id, attachments)
		// Recreate knowledge contents and their attachments
		await createContent(tx, id, contents)

		if (emails && emails.length > 0) {
			if (rest.access === KnowledgeAccess.EMAIL) {
				await tx.userKnowledge.deleteMany({
					where: {
						knowledgeId: id,
					},
				})
				await createEmails(tx, id, emails)
			}
		}

		return knowledge
	})
}

export async function deleteById(id: string) {
	return await prisma.knowledge.delete({
		where: {
			id,
		},
	})
}

export async function updateStatus(
	id: string,
	userId: string,
	status: KnowledgeStatus,
	action: KnowledgeActivityLogAction,
	comment?: string,
) {
	return await prisma.$transaction(async (tx) => {
		const knowledge = await tx.knowledge.update({
			where: { id },
			data: { status, rejectionComment: comment },
		})

		await tx.knowledgeActivityLog.create({
			data: {
				id: ulid(),
				knowledgeId: id,
				action,
				createdByUserId: userId,
			},
		})

		return knowledge
	})
}

export async function createAttachments(
	tx: Prisma.TransactionClient,
	knowledgeId: string,
	attachments: KnowledgeAttachmentDTO[],
) {
	if (attachments && attachments.length > 0) {
		await tx.knowledgeAttachment.createMany({
			data: attachments.map((attachment) => ({
				id: ulid(),
				knowledgeId: knowledgeId,
				attachmentUrl: attachment.attachmentUrl,
			})),
		})
	}
}

export async function createContent(
	tx: Prisma.TransactionClient,
	knowledgeId: string,
	contents: KnowledgeContentDTO[],
) {
	const knowledgeContentCreateManyInput: Prisma.KnowledgeContentCreateManyInput[] =
		[]
	const knowledgeContentAttachmentCreateManyInput: Prisma.KnowledgeContentAttachmentCreateManyInput[] =
		[]
	// Create knowledge contents and their attachments
	for (const content of contents) {
		const contentId = ulid()

		knowledgeContentCreateManyInput.push({
			id: contentId,
			knowledgeId: knowledgeId,
			title: content.title,
			description: content.description,
			order: content.order,
		})

		// Create content attachments if exists
		if (content.attachments && content.attachments.length > 0) {
			knowledgeContentAttachmentCreateManyInput.push(
				...content.attachments.map((attachment) => ({
					id: ulid(),
					knowledgeContentId: contentId,
					order: attachment.order,
					attachmentUrl: attachment.attachmentUrl,
				})),
			)
		}
	}

	await tx.knowledgeContent.createMany({
		data: knowledgeContentCreateManyInput,
	})

	await tx.knowledgeContentAttachment.createMany({
		data: knowledgeContentAttachmentCreateManyInput,
	})
}

export async function createEmails(
	tx: Prisma.TransactionClient,
	knowledgeId: string,
	emails: string[],
) {
	for (const email of emails) {
		const userId = await prisma.user.findUnique({
			where: {
				email: email,
			},
		})
		if (userId) {
			await tx.userKnowledge.create({
				data: {
					id: ulid(),
					knowledgeId: knowledgeId,
					userId: userId.id,
				},
			})
		}
	}
}

export async function incrementTotalViews(id: string) {
	await prisma.$queryRaw`
        SELECT *
        FROM "Knowledge"
        WHERE id = ${id}
        FOR UPDATE
    `
	return await prisma.knowledge.update({
		where: { id },
		data: { totalViews: { increment: 1 } },
	})
}

export async function archiveOrUnarchiveKnowledge(
	id: string,
	isArchived: boolean,
) {
	return await prisma.knowledge.update({
		where: { id },
		data: { isArchived: isArchived },
	})
}

export async function getAllVersionsById(id: string) {
	// Use recursive CTE to get all versions in a single query
	// Only fetch id, version, and headline
	const versions = await prisma.$queryRaw<
		{ id: string; version: number; headline: string }[]
	>`
        WITH RECURSIVE 
        -- First, find the root by traversing up
        ancestors AS (
            SELECT id, "parentId"
            FROM "Knowledge"
            WHERE id = ${id}
            
            UNION ALL
            
            SELECT k.id, k."parentId"
            FROM "Knowledge" k
            INNER JOIN ancestors a ON k.id = a."parentId"
        ),
        -- Get the root id (the one with no parent)
        root AS (
            SELECT id FROM ancestors WHERE "parentId" IS NULL
        ),
        -- Now traverse down from root to get all descendants
        all_versions AS (
            SELECT id, version, headline, "parentId"
            FROM "Knowledge"
            WHERE id = (SELECT id FROM root)
            
            UNION ALL
            
            SELECT k.id, k.version, k.headline, k."parentId"
            FROM "Knowledge" k
            INNER JOIN all_versions av ON k."parentId" = av.id
        )
        SELECT id, version, headline
        FROM all_versions
        ORDER BY version ASC
    `

	return versions
}

// ... imports prisma dan lain-lain

// 1. Function untuk menyimpan data Share
export async function createShare(
	knowledgeId: string,
	senderUserId: string,
	note: string | undefined,
	recipientsData: { email: string; userId: string | null }[],
) {
	return prisma.knowledgeShare.create({
		data: {
			knowledgeId,
			sharedByUserId: senderUserId,
			note,
			recipients: {
				create: recipientsData.map((r) => ({
					recipientEmail: r.email,
					recipientUserId: r.userId,
				})),
			},
		},
		include: {
			knowledge: {
				select: { headline: true },
			},
			recipients: true,
		},
	})
}

export async function getShareHistory(
	userId: string,
	filters: EzFilter.FilteringQuery,
) {
	return prisma.knowledgeShare.findMany({
		where: {
			sharedByUserId: userId,
		},
		include: {
			knowledge: {
				select: {
					id: true,
					headline: true,
					status: true,
				},
			},
			recipients: {
				include: {
					recipientUser: {
						select: {
							fullName: true,
							profilePictureUrl: true,
						},
					},
				},
			},
		},
		orderBy: {
			createdAt: "desc",
		},
	})
}

export async function findUsersByEmails(emails: string[]) {
	return prisma.user.findMany({
		where: {
			email: { in: emails },
			isActive: true,
		},
		select: {
			id: true,
			email: true,
		},
	})
}
