import { CreateGoogleUserDTO, CreateUserDTO } from "$entities/UserManagement"
import { exclude, UpdateUserDTO } from "$entities/User"
import { prisma } from "$pkg/prisma"
import * as EzFilter from "@nodewave/prisma-ezfilter"

export async function create(data: CreateUserDTO) {
	return await prisma.user.create({ data })
}

export async function createGoogleUser(data: CreateGoogleUserDTO) {
	return await prisma.user.create({ data })
}

export async function getById(id: string) {
	return await prisma.user.findUnique({
		where: { id },
	})
}

export async function update(id: string, data: UpdateUserDTO) {
	return await prisma.user.update({
		where: { id },
		data,
	})
}

export async function deleteById(id: string) {
	return await prisma.$transaction(async (tx) => {
		// Remove all tenant associations
		await tx.tenantUser.deleteMany({
			where: { userId: id },
		})

		// Soft delete user
		return await tx.user.update({
			where: { id },
			data: { isActive: false },
		})
	})
}

export async function restoreById(id: string) {
	return await prisma.user.update({
		where: { id },
		data: { isActive: true },
	})
}

export async function getAll(filters: EzFilter.FilteringQuery) {
	const queryBuilder = new EzFilter.BuildQueryFilter()
	const { filters: rawFilters, ...rest } = filters
	const usedFilters = queryBuilder.build(rest as any)

	const where: any = {
		...usedFilters.query.where,
	}

	// Handle filters (exact match for specific fields)
	if (rawFilters) {
		let parsedFilters: Array<{ key: string; value: any }> = []
		if (typeof rawFilters === "string") {
			try {
				parsedFilters = JSON.parse(rawFilters)
			} catch (e) {
				/* ignore */
			}
		} else if (Array.isArray(rawFilters)) {
			parsedFilters = rawFilters
		}

		for (const filter of Object.values(parsedFilters)) {
			const { key, value } = filter as any
			if (!key) continue

			if (key === "isActive") {
				where.isActive = String(value) === "true" || value === true
			} else if (key === "email") {
				where.email = { contains: value, mode: "insensitive" }
			} else if (key === "tenantId") {
				// For UserRepository.getAll, tenantId filtering is via tenantUser association
				where.tenantUser = { some: { tenantId: value } }
			} else if (key === "_scopedTenantId") {
				// If user manually filters by tenantId on top of scope, we respect it (AND logic)
				// But base visibility is: (In Tenant OR No Tenant)
				where.OR = [
					{ tenantUser: { some: { tenantId: value } } },
					{ tenantUser: { none: {} } },
				]
			} else if (key === "tenantRoleId") {
				where.tenantUser = { some: { tenantRoleId: value } }
			} else if (key === "isJoined") {
				if (String(value) === "true" || value === true) {
					where.tenantUser = { some: {} }
				} else {
					where.tenantUser = { none: {} }
				}
			}
		}
	}

	const [user, totalData] = await Promise.all([
		prisma.user.findMany({
			...usedFilters.query,
			where,
			include: {
				tenantUser: {
					include: {
						tenant: true,
						tenantRole: true,
					},
				},
			},
		} as any),
		prisma.user.count({
			where,
		}),
	])

	let totalPage = 1
	if (totalData > usedFilters.query.take)
		totalPage = Math.ceil(totalData / usedFilters.query.take)

	return {
		entries: user.map((u) => exclude(u, "password")),
		totalData,
		totalPage,
	}
}

export async function getInvitableForTenant(
	tenantId: string,
	filters: EzFilter.FilteringQuery,
) {
	const queryBuilder = new EzFilter.BuildQueryFilter()
	const { filters: rawFilters, ...rest } = filters
	const usedFilters = queryBuilder.build(rest as any)

	const where: any = {
		...usedFilters.query.where,
		tenantUser: { none: { tenantId } },
	}

	if (rawFilters) {
		let parsedFilters: Array<{ key: string; value: any }> = []
		if (typeof rawFilters === "string") {
			try {
				parsedFilters = JSON.parse(rawFilters)
			} catch (e) {
				/* ignore */
			}
		} else if (Array.isArray(rawFilters)) {
			parsedFilters = rawFilters
		}

		for (const filter of Object.values(parsedFilters)) {
			const { key, value } = filter as any
			if (!key) continue

			if (key === "isActive") {
				where.isActive = String(value) === "true" || value === true
			} else if (key === "email") {
				where.email = { contains: value, mode: "insensitive" }
			} else if (key === "fullName") {
				where.fullName = { contains: value, mode: "insensitive" }
			}
		}
	}

	const [users, totalData] = await Promise.all([
		prisma.user.findMany({
			...usedFilters.query,
			where,
			include: {
				tenantUser: {
					include: {
						tenant: true,
						tenantRole: true,
					},
				},
			},
		} as any),
		prisma.user.count({ where }),
	])

	let totalPage = 1
	if (totalData > usedFilters.query.take)
		totalPage = Math.ceil(totalData / usedFilters.query.take)

	return {
		entries: users.map((u) => exclude(u, "password")),
		totalData,
		totalPage,
	}
}

export async function getByEmail(email: string) {
	return await prisma.user.findUnique({
		where: { email },
	})
}

export async function updatePassword(id: string, password: string) {
	return await prisma.user.update({
		where: { id },
		data: { password },
	})
}

export async function getMe(userId: string) {
	return await prisma.user.findUnique({
		where: { id: userId },
		include: {
			tenantUser: {
				select: {
					id: true,
					tenantId: true,
					tenantRoleId: true,
				},
			},
		},
	})
}
