import { ulid } from "ulid"
import {
	PrismaClient,
	KnowledgeStatus,
	KnowledgeActivityLogAction,
	Roles,
	KnowledgeType,
	KnowledgeAccess,
} from "../../generated/prisma/client"

export async function seedKnowledge(prisma: PrismaClient) {
	const count = await prisma.knowledge.count()

	if (count == 0) {
		const [tenant, user] = await Promise.all([
			prisma.tenant.findFirst(),
			prisma.user.findFirst({
				where: {
					role: Roles.ADMIN,
				},
			}),
		])

		const userInTenant = await prisma.user.findFirst({
			where: {
				role: Roles.USER,
			},
		})
		if (!tenant) {
			console.log("Tenant not found. Please seed tenant first.")
			return
		}

		if (!user) {
			console.log("Admin user not found. Please seed admin first.")
			return
		}

		// Data knowledge seed
		const knowledgeData = [
			{
				id: ulid(),
				tenantId: tenant.id,
				category: "Category 1",
				subCategory: "Sub Category 1",
				case: "Case 1",
				type: KnowledgeType.ARTICLE,
				access: KnowledgeAccess.EMAIL,
				headline: "Cara Menangani Komplain Pelanggan",
				status: KnowledgeStatus.APPROVED,
				createdByUserId: user.id,
				attachments: [
					{
						id: ulid(),
						attachmentUrl: "https://example.com/attachment1.pdf",
					},
				],
				contents: [
					{
						id: ulid(),
						title: "Langkah 1: Dengarkan Keluhan",
						description:
							"Dengarkan keluhan pelanggan dengan seksama tanpa memotong pembicaraan. Tunjukkan empati dan pengertian terhadap situasi yang mereka alami.",
						order: 1,
						contentAttachments: [
							{
								id: ulid(),
								order: 1,
								attachmentUrl: "https://example.com/content1-attachment1.jpg",
							},
						],
					},
					{
						id: ulid(),
						title: "Langkah 2: Identifikasi Masalah",
						description:
							"Identifikasi akar masalah dengan mengajukan pertanyaan yang relevan. Pastikan Anda memahami masalah dengan baik sebelum memberikan solusi.",
						order: 2,
						contentAttachments: [
							{
								id: ulid(),
								order: 1,
								attachmentUrl: "https://example.com/content2-attachment1.jpg",
							},
							{
								id: ulid(),
								order: 2,
								attachmentUrl: "https://example.com/content2-attachment2.jpg",
							},
						],
					},
					{
						id: ulid(),
						title: "Langkah 3: Berikan Solusi",
						description:
							"Tawarkan solusi yang konkret dan dapat dilaksanakan. Jelaskan langkah-langkah yang akan diambil untuk menyelesaikan masalah.",
						order: 3,
						contentAttachments: [],
					},
				],
			},
			{
				id: ulid(),
				tenantId: tenant.id,
				category: "Category 2",
				subCategory: "Sub Category 2",
				case: "Case 2",
				headline: "Panduan Penggunaan Sistem CRM",
				status: KnowledgeStatus.PENDING,
				type: KnowledgeType.ARTICLE,
				access: KnowledgeAccess.TENANT,
				createdByUserId: user.id,
				attachments: [],
				contents: [
					{
						id: ulid(),
						title: "Pendahuluan CRM",
						description:
							"Sistem CRM (Customer Relationship Management) adalah tools yang digunakan untuk mengelola interaksi dengan pelanggan. Sistem ini membantu meningkatkan pelayanan dan kepuasan pelanggan.",
						order: 1,
						contentAttachments: [
							{
								id: ulid(),
								order: 1,
								attachmentUrl: "https://example.com/crm-intro.png",
							},
						],
					},
					{
						id: ulid(),
						title: "Cara Login ke Sistem",
						description:
							"Untuk login ke sistem CRM, masukkan username dan password yang telah diberikan. Pastikan koneksi internet Anda stabil.",
						order: 2,
						contentAttachments: [],
					},
				],
			},
			{
				id: ulid(),
				tenantId: null,
				type: KnowledgeType.CASE,
				access: KnowledgeAccess.PUBLIC,
				category: "Category 3",
				subCategory: "Sub Category 3",
				case: "Case 3",
				headline: "Prosedur Eskalasi Masalah",
				status: KnowledgeStatus.REVISION,
				createdByUserId: user.id,
				attachments: [
					{
						id: ulid(),
						attachmentUrl: "https://example.com/eskalasi-flowchart.pdf",
					},
					{
						id: ulid(),
						attachmentUrl: "https://example.com/eskalasi-template.docx",
					},
				],
				contents: [
					{
						id: ulid(),
						title: "Kapan Harus Eskalasi",
						description:
							"Eskalasi dilakukan ketika masalah tidak dapat diselesaikan di level Anda atau memerlukan persetujuan dari atasan. Pastikan semua informasi terdokumentasi dengan baik.",
						order: 1,
						contentAttachments: [],
					},
					{
						id: ulid(),
						title: "Proses Eskalasi",
						description:
							"Hubungi supervisor atau manager melalui channel yang telah ditentukan. Berikan informasi lengkap mengenai kasus yang perlu dieskalasi.",
						order: 2,
						contentAttachments: [
							{
								id: ulid(),
								order: 1,
								attachmentUrl: "https://example.com/eskalasi-process.png",
							},
						],
					},
					{
						id: ulid(),
						title: "Follow Up",
						description:
							"Lakukan follow up secara berkala untuk memastikan masalah telah ditangani dengan baik.",
						order: 3,
						contentAttachments: [],
					},
				],
			},
		]

		// Create knowledge dengan transaction
		for (const knowledge of knowledgeData) {
			await prisma.$transaction(async (tx) => {
				// Create knowledge
				await tx.knowledge.create({
					data: {
						id: knowledge.id,
						tenantId: knowledge.tenantId,
						type: knowledge.type,
						access: knowledge.access,
						category: knowledge.category,
						subCategory: knowledge.subCategory,
						case: knowledge.case,
						headline: knowledge.headline,
						status: knowledge.status,
						createdByUserId: knowledge.createdByUserId,
					},
				})

				// Create knowledge attachments
				if (knowledge.attachments.length > 0) {
					await tx.knowledgeAttachment.createMany({
						data: knowledge.attachments.map((attachment) => ({
							...attachment,
							knowledgeId: knowledge.id,
						})),
					})
				}

				// Create knowledge contents
				for (const content of knowledge.contents) {
					await tx.knowledgeContent.create({
						data: {
							id: content.id,
							knowledgeId: knowledge.id,
							title: content.title,
							description: content.description,
							order: content.order,
						},
					})

					// Create content attachments
					if (content.contentAttachments.length > 0) {
						await tx.knowledgeContentAttachment.createMany({
							data: content.contentAttachments.map((attachment) => ({
								...attachment,
								knowledgeContentId: content.id,
							})),
						})
					}
				}

				if (knowledge.access === KnowledgeAccess.EMAIL) {
					await tx.userKnowledge.create({
						data: {
							id: ulid(),
							knowledgeId: knowledge.id,
							userId: userInTenant!.id,
						},
					})
				}

				// Create activity log
				await tx.knowledgeActivityLog.create({
					data: {
						id: ulid(),
						knowledgeId: knowledge.id,
						action: KnowledgeActivityLogAction.CREATE,
						createdByUserId: knowledge.createdByUserId,
					},
				})

				// Create additional activity log based on status
				if (knowledge.status === KnowledgeStatus.APPROVED) {
					await tx.knowledgeActivityLog.create({
						data: {
							id: ulid(),
							knowledgeId: knowledge.id,
							action: KnowledgeActivityLogAction.APPROVE,
							createdByUserId: knowledge.createdByUserId,
						},
					})
				} else if (knowledge.status === KnowledgeStatus.REVISION) {
					await tx.knowledgeActivityLog.create({
						data: {
							id: ulid(),
							knowledgeId: knowledge.id,
							action: KnowledgeActivityLogAction.REVISION,
							createdByUserId: knowledge.createdByUserId,
						},
					})
				}
			})

			console.log(`Knowledge "${knowledge.headline}" seeded successfully`)
		}

		console.log(
			`✅ All ${knowledgeData.length} knowledge entries seeded successfully`,
		)
	} else {
		console.log("Knowledge already seeded")
	}
}
