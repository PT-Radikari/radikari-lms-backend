import {
	Knowledge,
	KnowledgeAccess,
	KnowledgeActivityLogAction,
	KnowledgeAttachment,
	KnowledgeStatus,
	KnowledgeType,
	NotificationType,
	Prisma,
	UserKnowledge,
} from "../../generated/prisma/client"
import {
	KnowledgeApprovalDTO,
	KnowledgeBulkCreateDTO,
	KnowledgeDTO,
	KnowledgeQueueDTO,
} from "$entities/Knowledge"
import * as EzFilter from "@nodewave/prisma-ezfilter"
import * as KnowledgeRepository from "$repositories/KnowledgeRepository"
import {
	HandleServiceResponseCustomError,
	HandleServiceResponseSuccess,
	ResponseStatus,
	ServiceResponse,
} from "$entities/Service"
import Logger from "$pkg/logger"
import { UserJWTDAO } from "$entities/User"
import { PUBSUB_TOPICS } from "$entities/PubSub"
import { GlobalPubSub } from "$pkg/pubsub"
import axios from "axios"
import * as XLSX from "xlsx"
import { ulid } from "ulid"
import * as TenantRepository from "$repositories/TenantRepository"
import * as OperationRepository from "$repositories/OperationRepository"
import * as UserActivityLogService from "$services/UserActivityLogService"
import * as NotificationService from "$services/NotificationService"
import { KnowledgeShareDTO } from "$entities/Knowledge"
import { prisma } from "$pkg/prisma"

export async function create(
	userId: string,
	tenantId: string,
	data: KnowledgeDTO,
): Promise<ServiceResponse<Knowledge | {}>> {
	try {
		if (data.parentId) {
			const versions = await KnowledgeRepository.getAllVersionsById(
				data.parentId,
			)

			if (versions && versions.length > 0) {
				const highestVersion = versions[versions.length - 1].version
				data.version = highestVersion + 1
			} else {
				const parent = await KnowledgeRepository.getById(data.parentId)
				data.version = parent!.version + 1
			}
		}

		const createdData = await KnowledgeRepository.create(userId, tenantId, data)

		await UserActivityLogService.create(
			userId,
			"Menambahkan pengetahuan",
			tenantId,
			`dengan headline "${data.headline}"`,
		)

		return HandleServiceResponseSuccess(createdData)
	} catch (err) {
		Logger.error(`KnowledgeService.create : `, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function getAll(
	user: UserJWTDAO,
	tenantId: string,
	filters: EzFilter.FilteringQuery,
): Promise<ServiceResponse<EzFilter.PaginatedResult<Knowledge[]> | {}>> {
	try {
		const data = await KnowledgeRepository.getAll(user, tenantId, filters)
		return HandleServiceResponseSuccess(data)
	} catch (err) {
		Logger.error(`KnowledgeService.getAll`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function getAllArchived(
	user: UserJWTDAO,
	tenantId: string,
	filters: EzFilter.FilteringQuery,
): Promise<ServiceResponse<EzFilter.PaginatedResult<Knowledge[]> | {}>> {
	try {
		const data = await KnowledgeRepository.getAllArchived(
			user,
			tenantId,
			filters,
		)
		return HandleServiceResponseSuccess(data)
	} catch (err) {
		Logger.error(`KnowledgeService.getAll`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function getSummary(
	user: UserJWTDAO,
	tenantId: string,
	filters: EzFilter.FilteringQuery,
): Promise<ServiceResponse<{}>> {
	try {
		const data = await KnowledgeRepository.getSummary(user, tenantId, filters)

		return HandleServiceResponseSuccess(data)
	} catch (err) {
		Logger.error(`KnowledgeService.getSummary`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function getById(
	id: string,
	tenantId: string,
	userId: string,
): Promise<ServiceResponse<Knowledge | {}>> {
	try {
		let knowledge = await KnowledgeRepository.getById(id)

		if (!knowledge)
			return HandleServiceResponseCustomError(
				"Invalid ID",
				ResponseStatus.NOT_FOUND,
			)

		if (knowledge.createdByUserId !== userId) {
			await KnowledgeRepository.incrementTotalViews(id)
		}

		return HandleServiceResponseSuccess(knowledge)
	} catch (err) {
		Logger.error(`KnowledgeService.getById`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function getAllVersionsById(
	id: string,
): Promise<
	ServiceResponse<{ id: string; version: number; headline: string }[] | {}>
> {
	try {
		const versions = await KnowledgeRepository.getAllVersionsById(id)

		if (!versions || versions.length === 0)
			return HandleServiceResponseCustomError(
				"Knowledge not found",
				ResponseStatus.NOT_FOUND,
			)

		return HandleServiceResponseSuccess(versions)
	} catch (err) {
		Logger.error(`KnowledgeService.getAllVersionsById`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export type UpdateResponse = Knowledge | {}
export async function update(
	id: string,
	tenantId: string,
	data: KnowledgeDTO,
	userId: string,
): Promise<ServiceResponse<UpdateResponse>> {
	try {
		const knowledge = await KnowledgeRepository.getById(id)

		if (!knowledge)
			return HandleServiceResponseCustomError(
				"Invalid ID",
				ResponseStatus.NOT_FOUND,
			)

		let status = knowledge.status

		if (status == "REJECTED" || status == "REVISION") {
			status = "PENDING"
		}

		const updatedKnowledge = await KnowledgeRepository.update(
			id,
			data,
			tenantId,
			status,
		)

		if (updatedKnowledge.status == KnowledgeStatus.APPROVED) {
			const pubsub = GlobalPubSub.getInstance().getPubSub()
			const knowledgeWithUserKnowledgeAndKnowledgeAttachment =
				await KnowledgeRepository.getById(id)

			try {
				await pubsub.sendToQueue(
					PUBSUB_TOPICS.KNOWLEDGE_UPDATE,
					generateKnowledgeQueueDTO(
						knowledgeWithUserKnowledgeAndKnowledgeAttachment as any,
					),
				)
			} catch (mqError) {
				Logger.warning(
					"KnowledgeService.update: Failed to publish update event",
					{
						error: mqError,
						knowledgeId: id,
					},
				)
			}
		}

		await UserActivityLogService.create(
			userId,
			"Mengedit pengetahuan",
			tenantId,
			`dengan headline "${updatedKnowledge.headline}"`,
		)

		return HandleServiceResponseSuccess(updatedKnowledge)
	} catch (err) {
		Logger.error(`KnowledgeService.update`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function deleteById(
	id: string,
	tenantId: string,
	userId: string,
): Promise<ServiceResponse<{}>> {
	try {
		const knowledge = await KnowledgeRepository.getById(id)

		if (!knowledge)
			return HandleServiceResponseCustomError(
				"Invalid ID",
				ResponseStatus.NOT_FOUND,
			)

		await KnowledgeRepository.deleteById(id)
		const pubsub = GlobalPubSub.getInstance().getPubSub()
		try {
			await pubsub.sendToQueue(PUBSUB_TOPICS.KNOWLEDGE_DELETE, {
				knowledgeId: id,
			})
		} catch (mqError) {
			Logger.warning(
				"KnowledgeService.deleteById: Failed to publish delete event",
				{
					error: mqError,
					knowledgeId: id,
				},
			)
		}

		await UserActivityLogService.create(
			userId,
			"Menghapus pengetahuan",
			tenantId,
			`dengan headline "${knowledge.headline}"`,
		)

		return HandleServiceResponseSuccess({})
	} catch (err) {
		Logger.error(`KnowledgeService.deleteById`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function approveById(
	id: string,
	tenantId: string,
	userId: string,
	data: KnowledgeApprovalDTO,
): Promise<ServiceResponse<{}>> {
	try {
		const knowledge = await KnowledgeRepository.getById(id)

		if (!knowledge)
			return HandleServiceResponseCustomError(
				"Invalid ID",
				ResponseStatus.NOT_FOUND,
			)

		let status: KnowledgeStatus

		switch (data.action) {
			case KnowledgeActivityLogAction.APPROVE:
				status = KnowledgeStatus.APPROVED
				break
			case KnowledgeActivityLogAction.REJECT:
				status = KnowledgeStatus.REJECTED
				break
			case KnowledgeActivityLogAction.REVISION:
				status = KnowledgeStatus.REVISION
				break
			default:
				status = KnowledgeStatus.PENDING
				break
		}
		if (status == knowledge.status) {
			return HandleServiceResponseCustomError(
				`Knowledge is already in status ${status}`,
				ResponseStatus.BAD_REQUEST,
			)
		}

		await KnowledgeRepository.updateStatus(
			id,
			userId,
			status,
			data.action,
			data.comment,
		)

		if (data.action == KnowledgeActivityLogAction.APPROVE) {
			Logger.info(
				"KnowledgeService.approveById: Preparing to send message to queue.",
				{
					knowledgeId: knowledge.id,
					headline: knowledge.headline,
				},
			)

			const payload = generateKnowledgeQueueDTO(knowledge as any)

			Logger.info(
				"KnowledgeService.approveById: Generated payload for queue.",
				{
					payload: payload,
				},
			)

			const pubsub = GlobalPubSub.getInstance().getPubSub()

			try {
				await pubsub.sendToQueue(PUBSUB_TOPICS.KNOWLEDGE_CREATE, payload)
				await pubsub.sendToQueue(
					PUBSUB_TOPICS.KNOWLEDGE_APPROVAL_NOTIFICATION,
					{
						knowledgeId: knowledge.id,
						excludeUserId: userId,
					},
				)
			} catch (mqError) {
				Logger.warning(
					"KnowledgeService.approveById: Failed to publish approval events",
					{
						error: mqError,
						knowledgeId: knowledge.id,
					},
				)
			}
		}

		await UserActivityLogService.create(
			userId,
			"Menyetujui pengetahuan",
			tenantId,
			`dengan headline "${knowledge.headline}" dan status "${status}"`,
		)

		return HandleServiceResponseSuccess({})
	} catch (err) {
		Logger.error(`KnowledgeService.approveById`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

function generateKnowledgeQueueDTO(
	knowledge: Knowledge & { userKnowledge: UserKnowledge[] } & {
		knowledgeAttachment: KnowledgeAttachment[]
	},
): KnowledgeQueueDTO {
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

export async function sendKnowledgeApprovalNotification(
	knowledgeId: string,
	excludeUserId: string,
) {
	try {
		const knowledge = await KnowledgeRepository.getById(knowledgeId)

		if (!knowledge) {
			return
		}
		const notificationTitle = "Pengetahuan Baru Tersedia"
		const notificationMessage = `Pengetahuan baru "${knowledge.headline}" telah tersedia untuk dibaca.`

		switch (knowledge.access) {
			case KnowledgeAccess.TENANT:
				if (knowledge.tenantId) {
					await NotificationService.notifyTenantUsers(
						knowledge.tenantId,
						NotificationType.KNOWLEDGE_APPROVED,
						notificationTitle,
						notificationMessage,
						knowledge.id,
						excludeUserId,
					)
				}
				break

			case KnowledgeAccess.EMAIL:
				const userIds = knowledge.userKnowledge
					.map((uk) => uk.user.id)
					.filter((id) => id !== excludeUserId)

				if (userIds.length > 0) {
					await NotificationService.notifySpecificUsers(
						userIds,
						knowledge.tenantId ?? undefined,
						NotificationType.KNOWLEDGE_APPROVED,
						notificationTitle,
						notificationMessage,
						knowledge.id,
					)
				}
				break

			case KnowledgeAccess.PUBLIC:
				if (knowledge.tenantId) {
					await NotificationService.notifyTenantUsers(
						knowledge.tenantId,
						NotificationType.KNOWLEDGE_APPROVED,
						notificationTitle,
						notificationMessage,
						knowledge.id,
						excludeUserId,
					)
				}
				break
		}

		Logger.info(
			"KnowledgeService.sendKnowledgeApprovalNotification: Notifications sent",
			{
				knowledgeId: knowledge.id,
				access: knowledge.access,
			},
		)
	} catch (err) {
		Logger.error(
			"KnowledgeService.sendKnowledgeApprovalNotification: Failed to send notifications",
			{
				error: err,
				knowledgeId: knowledgeId,
			},
		)
	}
}

export async function bulkCreate(data: KnowledgeBulkCreateDTO, userId: string) {
	try {
		const fileUrls = data.fileUrls
		Logger.info("bulkCreate: Starting with", {
			fileUrls,
			type: data.type,
			access: data.access,
		})

		// Separate Excel files from PDF/DOCX files
		const excelFiles = fileUrls.filter((url) => {
			const ext = url.split(".").pop()?.toLowerCase() || ""
			return ["xlsx", "xls", "csv"].includes(ext)
		})

		const documentFiles = fileUrls.filter((url) => {
			const ext = url.split(".").pop()?.toLowerCase() || ""
			return ["pdf", "docx"].includes(ext)
		})

		Logger.info("bulkCreate: Excel files found", { excelFiles, documentFiles })

		// Process ALL files as attachments to a SINGLE knowledge entry
		const allFiles = [...excelFiles, ...documentFiles]

		if (allFiles.length > 0) {
			const knowledgeId = ulid()
			const knowledgeCreateInput: Prisma.KnowledgeCreateManyInput = {
				id: knowledgeId,
				tenantId: data.tenantId,
				createdByUserId: userId,
				access: data.access,
				type: data.type as KnowledgeType,
				headline: `Bulk Upload - ${new Date().toLocaleDateString("id-ID")}`,
				status: KnowledgeStatus.PENDING,
			}

			await KnowledgeRepository.createMany([knowledgeCreateInput])

			// Create attachments for ALL files
			const attachmentInputs: Prisma.KnowledgeAttachmentCreateManyInput[] =
				allFiles.map((fileUrl) => ({
					id: ulid(),
					knowledgeId: knowledgeId,
					attachmentUrl: fileUrl,
				}))
			await KnowledgeRepository.createManyAttachments(attachmentInputs)

			// Create content
			const contentInput: Prisma.KnowledgeContentCreateManyInput = {
				id: ulid(),
				knowledgeId: knowledgeId,
				title: "Uploaded Files",
				description: `${allFiles.length} file(s) uploaded via bulk upload`,
				order: 1,
			}
			await KnowledgeRepository.createManyContent([contentInput])

			Logger.info("bulkCreate: Completed successfully", {
				knowledgeId,
				filesCount: allFiles.length,
			})
		} else {
			Logger.info("bulkCreate: No files to process", {})
		}

		return HandleServiceResponseSuccess({})
	} catch (err) {
		Logger.error(`KnowledgeService.bulkCreate`, {
			error: err,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function bulkCreateTypeCase(
	data: KnowledgeBulkCreateDTO,
	userId: string,
) {
	try {
		// Only process the first Excel file
		const excelFiles = data.fileUrls.filter((url) => {
			const ext = url.split(".").pop()?.toLowerCase() || ""
			return ["xlsx", "xls", "csv"].includes(ext)
		})

		if (excelFiles.length === 0) {
			return HandleServiceResponseSuccess({})
		}

		const file = await axios.get(excelFiles[0], {
			responseType: "arraybuffer",
		})

		const responseData = file.data
		const workbook = XLSX.read(responseData, { type: "buffer" })

		// Fallback: use 3rd sheet if exists, otherwise try "Knowledge" or "Case", then finally first sheet
		let knowledges = workbook.Sheets[workbook.SheetNames[2]]
		if (!knowledges) {
			knowledges =
				workbook.Sheets["Knowledge"] ||
				workbook.Sheets["Case"] ||
				workbook.Sheets[workbook.SheetNames[0]]
		}

		const rowData: any[] = XLSX.utils.sheet_to_json(knowledges)

		const knowledgeCreateManyInput: Prisma.KnowledgeCreateManyInput[] = []
		const knwoledgeContentCreateManyInput: Prisma.KnowledgeContentCreateManyInput[] =
			[]

		for (const rawRow of rowData) {
			// Normalize row keys
			const row: any = {}
			for (const key in rawRow) {
				row[key.trim().toLowerCase()] = rawRow[key]
			}

			const tenantName =
				row["tenant name"] || row["tenant"] || row["nama tenant"] || row["unit"]
			const headline =
				row["detail case"] ||
				row["headline"] ||
				row["title"] ||
				row["judul"] ||
				row["nama pengetahuan"] ||
				row["topik"]
			const caseName = row["case"] || ""
			const category = row["category"] || ""
			const subCategory = row["sub category"] || row["subcategory"] || ""

			if (!headline) {
				Logger.warning(
					"KnowledgeService.bulkCreateTypeCase: Skipping row due to missing headline column (expected: headline, title, judul, or topik)",
					{
						foundHeaders: Object.keys(row),
						row: rawRow,
					},
				)
				continue
			}

			let tenantId: string | undefined = data.tenantId
			if (tenantName && tenantName !== "") {
				let tenant = await TenantRepository.getByName(tenantName)

				if (!tenant) {
					const operation = await OperationRepository.findFirst()

					tenant = await TenantRepository.create({
						id: ulid(),
						name: tenantName,
						description: tenantName,
						operationId: operation!.id,
						headOfTenantUserId: userId,
					})
				}

				tenantId = tenant.id
			}

			const knowledgeId = ulid()
			knowledgeCreateManyInput.push({
				id: knowledgeId,
				tenantId: tenantId,
				createdByUserId: userId,
				access: data.access,
				type: data.type as KnowledgeType,
				headline: headline,
				case: caseName,
				category: category,
				subCategory: subCategory,
				status: KnowledgeStatus.PENDING,
			})

			let order = 1

			if (row["merchant name"] && row["merchant name"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "Merchant Name",
					description: row["merchant name"],
					order: order++,
				})
			}

			if (row["aggregator name"] && row["aggregator name"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "Aggregator Name",
					description: row["aggregator name"],
					order: order++,
				})
			}

			if (row["probing"] && row["probing"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "Probing",
					description: row["probing"],
					order: order++,
				})
			}

			if (row["need kba?"] && row["need kba?"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "NEED KBA?",
					description: row["need kba?"],
					order: order++,
				})
			}

			if (row["fcr"] && row["fcr"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "FCR",
					description: row["fcr"],
					order: order++,
				})
			}

			if (row["guidance"] && row["guidance"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "Guidance",
					description: row["guidance"],
					order: order++,
				})
			}

			if (row["note"] && row["note"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "Note",
					description: row["note"],
					order: order++,
				})
			}

			if (row["sla escalation"] && row["sla escalation"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "SLA ESCALATION",
					description: row["sla escalation"],
					order: order++,
				})
			}

			if (row["assign"] && row["assign"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "Assign",
					description: row["assign"],
					order: order++,
				})
			}

			if (row["keterangan"] && row["keterangan"] !== "") {
				knwoledgeContentCreateManyInput.push({
					id: ulid(),
					knowledgeId: knowledgeId,
					title: "Keterangan",
					description: row["keterangan"],
					order: order++,
				})
			}
		}

		// Error if no valid rows found
		if (knowledgeCreateManyInput.length === 0) {
			Logger.error("bulkCreateTypeCase: No valid rows found in Excel file", {
				totalRows: rowData.length,
				headers: Object.keys(rowData[0] || {}),
				expectedColumn:
					"headline (or title, judul, nama pengetahuan, topik, detail case)",
			})
			return HandleServiceResponseCustomError(
				"Tidak ada data yang valid di Excel. Pastikan ada kolom 'headline' atau 'detail case' dengan data.",
				400,
			)
		}

		await KnowledgeRepository.createMany(knowledgeCreateManyInput)
		await KnowledgeRepository.createManyContent(knwoledgeContentCreateManyInput)

		// FIX: Also attach the uploaded Excel file to all created knowledge entries
		if (excelFiles.length > 0) {
			const excelFileAttachmentInput: Prisma.KnowledgeAttachmentCreateManyInput[] =
				knowledgeCreateManyInput.map((knowledge) => ({
					id: ulid(),
					knowledgeId: knowledge.id as string,
					attachmentUrl: excelFiles[0],
				}))
			await KnowledgeRepository.createManyAttachments(excelFileAttachmentInput)
		}

		const knowledgeIds = knowledgeCreateManyInput.map((k) => k.id as string)
		const createdKnowledges = await KnowledgeRepository.getByIds(knowledgeIds)
		const pubsub = GlobalPubSub.getInstance().getPubSub()

		for (const knowledge of createdKnowledges) {
			const payload = generateKnowledgeQueueDTO(knowledge as any)
			try {
				await pubsub.sendToQueue(PUBSUB_TOPICS.KNOWLEDGE_CREATE, payload)
			} catch (mqError) {
				Logger.warning(
					"KnowledgeService.bulkCreateTypeCase: Failed to publish create event",
					{
						error: mqError,
						knowledgeId: knowledge.id,
					},
				)
			}
		}

		return HandleServiceResponseSuccess({})
	} catch (error) {
		Logger.error(`KnowledgeService.bulkCreateTypeCase`, {
			error: error,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function archiveOrUnarchiveKnowledge(
	id: string,
	userId: string,
	tenantId: string,
) {
	try {
		const knowledge = await KnowledgeRepository.getById(id)

		if (!knowledge)
			return HandleServiceResponseCustomError(
				"Invalid ID",
				ResponseStatus.NOT_FOUND,
			)

		await KnowledgeRepository.archiveOrUnarchiveKnowledge(
			id,
			!knowledge.isArchived,
		)
		await UserActivityLogService.create(
			userId,
			"Mengarsipkan pengetahuan",
			tenantId,
			`dengan headline "${knowledge.headline}"`,
		)
		return HandleServiceResponseSuccess({})
	} catch (error) {
		Logger.error(`KnowledgeService.archiveOrUnarchiveKnowledge`, {
			error: error,
		})
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function shareKnowledge(
	userId: string,
	tenantId: string,
	knowledgeId: string,
	data: KnowledgeShareDTO,
): Promise<ServiceResponse<{}>> {
	try {
		const knowledge = await KnowledgeRepository.getById(knowledgeId)
		if (!knowledge) {
			return HandleServiceResponseCustomError(
				"Knowledge not found",
				ResponseStatus.NOT_FOUND,
			)
		}

		const existingUsers = await KnowledgeRepository.findUsersByEmails(
			data.emails,
		)

		const recipientsPayload = data.emails.map((email) => {
			const matchedUser = existingUsers.find((u) => u.email === email)
			return {
				email: email,
				userId: matchedUser ? matchedUser.id : null,
			}
		})

		const shareRecord = await KnowledgeRepository.createShare(
			knowledgeId,
			userId,
			data.note,
			recipientsPayload,
		)

		await UserActivityLogService.create(
			userId,
			"Membagikan pengetahuan",
			tenantId,
			`berjudul "${knowledge.headline}" kepada ${data.emails.length} orang`,
		)

		return HandleServiceResponseSuccess(shareRecord)
	} catch (err) {
		Logger.error(`KnowledgeService.shareKnowledge`, { error: err })
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function getShareHistory(
	userId: string,
	tenantId: string,
	filters: EzFilter.FilteringQuery,
): Promise<ServiceResponse<{}>> {
	try {
		const whereClause: Prisma.KnowledgeShareWhereInput = {
			AND: [
				{
					knowledge: { tenantId: tenantId },
				},
				{
					OR: [
						{ sharedByUserId: userId },
						{
							recipients: {
								some: {
									recipientUserId: userId,
								},
							},
						},
					],
				},
			],
		}

		const [total, entries] = await prisma.$transaction([
			prisma.knowledgeShare.count({ where: whereClause }),
			prisma.knowledgeShare.findMany({
				where: whereClause,
				orderBy: {
					createdAt: "desc",
				},
				include: {
					knowledge: {
						select: {
							id: true,
							headline: true,
							type: true,
							status: true,
							tenantId: true,
						},
					},
					recipients: {
						include: {
							recipientUser: {
								select: {
									fullName: true,
									email: true,
								},
							},
						},
					},
					sharedByUser: {
						select: {
							fullName: true,
							email: true,
						},
					},
				},
			}),
		])

		return HandleServiceResponseSuccess({
			entries,
			totalData: total,
		})
	} catch (err) {
		Logger.error(`KnowledgeService.getShareHistory`, { error: err })
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}
