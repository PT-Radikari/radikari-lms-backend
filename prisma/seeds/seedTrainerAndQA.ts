import { ulid } from "ulid"
import { PrismaClient, Roles } from "../../generated/prisma/client"

export async function seedTrainerAndQA(prisma: PrismaClient) {
	console.log("Starting seedTrainerAndQA function...")

	// Get all tenants
	const tenants = await prisma.tenant.findMany()
	if (tenants.length === 0) {
		console.log("No tenants found. Please seed tenant first.")
		return
	}

	for (const tenant of tenants) {
		console.log(`  Seeding trainer & QA for tenant: ${tenant.name}`)

		// Get trainer and quality assurance tenant roles for this specific tenant
		const trainerRole = await prisma.tenantRole.findFirst({
			where: {
				identifier: "TRAINER",
				tenantId: tenant.id,
			},
		})

		const qualityAssuranceRole = await prisma.tenantRole.findFirst({
			where: {
				identifier: "QUALITY_ASSURANCE",
				tenantId: tenant.id,
			},
		})

		if (!trainerRole || !qualityAssuranceRole) {
			console.log(`    ⚠️ Roles for tenant ${tenant.name} not found. Skipping.`)
			continue
		}

		// Create/Update trainer user
		const trainerEmail = `trainer@test.com` // Static email for testing
		const hashedPasswordTrainer = await Bun.password.hash(
			"trainer1234",
			"argon2id",
		)

		const trainerUser = await prisma.user.upsert({
			where: { email: trainerEmail },
			update: {},
			create: {
				id: ulid(),
				fullName: `Trainer - ${tenant.name}`,
				password: hashedPasswordTrainer,
				email: trainerEmail,
				role: Roles.USER,
				phoneNumber: "08224567891",
			},
		})

		// Ensure tenant user association for trainer
		await prisma.tenantUser.upsert({
			where: {
				userId_tenantId: {
					userId: trainerUser.id,
					tenantId: tenant.id,
				},
			},
			update: {
				tenantRoleId: trainerRole.id,
			},
			create: {
				id: ulid(),
				userId: trainerUser.id,
				tenantId: tenant.id,
				tenantRoleId: trainerRole.id,
			},
		})

		// Create/Update QA user
		const qaEmail = `qa@test.com` // Static email for testing
		const hashedPasswordQA = await Bun.password.hash("qa1234", "argon2id")

		const qaUser = await prisma.user.upsert({
			where: { email: qaEmail },
			update: {},
			create: {
				id: ulid(),
				fullName: `Quality Assurance - ${tenant.name}`,
				password: hashedPasswordQA,
				email: qaEmail,
				role: Roles.USER,
				phoneNumber: "08224567892",
			},
		})

		// Ensure tenant user association for QA
		await prisma.tenantUser.upsert({
			where: {
				userId_tenantId: {
					userId: qaUser.id,
					tenantId: tenant.id,
				},
			},
			update: {
				tenantRoleId: qualityAssuranceRole.id,
			},
			create: {
				id: ulid(),
				userId: qaUser.id,
				tenantId: tenant.id,
				tenantRoleId: qualityAssuranceRole.id,
			},
		})
	}

	console.log("seedTrainerAndQA function completed successfully")
}
