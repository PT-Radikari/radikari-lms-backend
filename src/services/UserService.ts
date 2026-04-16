import {
	HandleServiceResponseCustomError,
	ResponseStatus,
	ServiceResponse,
} from "$entities/Service"
import {
	exclude,
	CreateUserDTO,
	UpdateUserDTO,
	UserJWTDAO,
} from "$entities/User"
import Logger from "$pkg/logger"
import * as UserRepository from "$repositories/UserRepository"
import * as EzFilter from "@nodewave/prisma-ezfilter"
import { User, Roles } from "../../generated/prisma/client"

export type CreateResponse = User | {}

export async function create(
	data: CreateUserDTO,
): Promise<ServiceResponse<CreateResponse>> {
	try {
		data.password = await Bun.password.hash(data.password, "argon2id")
		const user = await UserRepository.create(data)
		return {
			status: true,
			data: user,
		}
	} catch (err) {
		Logger.error(`UserService.create`, { error: err })
		return HandleServiceResponseCustomError("Internal Server Error", 500)
	}
}

export async function getAll(
	filters: EzFilter.FilteringQuery,
	currentUser?: UserJWTDAO,
): Promise<ServiceResponse<EzFilter.PaginatedResult<User[]> | {}>> {
	try {
		if (currentUser && currentUser.role !== Roles.ADMIN) {
			const userDetails = await UserRepository.getMe(currentUser.id)
			const tenantUsers = (userDetails as any)?.tenantUser || []

			if (tenantUsers.length > 0) {
				const tenantId = tenantUsers[0].tenantId

				let newFilters: any[] = []
				if (filters.filters) {
					if (typeof filters.filters === "string") {
						try {
							newFilters = JSON.parse(filters.filters)
						} catch {
							/* ignore */
						}
					} else if (Array.isArray(filters.filters)) {
						newFilters = [...filters.filters]
					}
				}

				newFilters.push({ key: "_scopedTenantId", value: tenantId })
				filters.filters = newFilters
			}
		}

		const data = await UserRepository.getAll(filters)
		return { status: true, data }
	} catch (err) {
		Logger.error(`UserService.getAll`, { error: err })
		return HandleServiceResponseCustomError(
			"Internal Server Error",
			ResponseStatus.INTERNAL_SERVER_ERROR,
		)
	}
}

export async function getInvitableForTenant(
	tenantId: string,
	filters: EzFilter.FilteringQuery,
): Promise<ServiceResponse<EzFilter.PaginatedResult<User[]> | {}>> {
	try {
		const data = await UserRepository.getInvitableForTenant(tenantId, filters)
		return { status: true, data }
	} catch (err) {
		Logger.error(`UserService.getInvitableForTenant`, { error: err })
		return HandleServiceResponseCustomError(
			"Internal Server Error",
			ResponseStatus.INTERNAL_SERVER_ERROR,
		)
	}
}

export async function getById(id: string): Promise<ServiceResponse<User | {}>> {
	try {
		const user = await UserRepository.getById(id)

		if (!user) {
			return HandleServiceResponseCustomError(
				"Invalid ID",
				ResponseStatus.BAD_REQUEST,
			)
		}

		return {
			status: true,
			data: exclude(user, "password"),
		}
	} catch (err) {
		Logger.error(`UserService.getById`, { error: err })
		return HandleServiceResponseCustomError(
			"Internal Server Error",
			ResponseStatus.INTERNAL_SERVER_ERROR,
		)
	}
}

export async function update(
	id: string,
	data: UpdateUserDTO,
): Promise<ServiceResponse<User | {}>> {
	try {
		const existing = await UserRepository.getById(id)

		if (!existing) {
			return HandleServiceResponseCustomError(
				"Invalid ID",
				ResponseStatus.BAD_REQUEST,
			)
		}

		const user = await UserRepository.update(id, data)
		return { status: true, data: user }
	} catch (err) {
		Logger.error(`UserService.update`, { error: err })
		return HandleServiceResponseCustomError(
			"Internal Server Error",
			ResponseStatus.INTERNAL_SERVER_ERROR,
		)
	}
}

export async function deleteById(id: string): Promise<ServiceResponse<{}>> {
	try {
		await UserRepository.deleteById(id)
		return { status: true, data: {} }
	} catch (err) {
		Logger.error(`UserService.deleteById`, { error: err })
		return HandleServiceResponseCustomError(
			"Internal Server Error",
			ResponseStatus.INTERNAL_SERVER_ERROR,
		)
	}
}

export async function restoreById(id: string): Promise<ServiceResponse<{}>> {
	try {
		await UserRepository.restoreById(id)
		return { status: true, data: {} }
	} catch (err) {
		Logger.error(`UserService.restoreById`, { error: err })
		return HandleServiceResponseCustomError(
			"Internal Server Error",
			ResponseStatus.INTERNAL_SERVER_ERROR,
		)
	}
}

// =========================
// ✅ NEW: GET ME (tenant check)
// =========================
export type GetMeResponse =
	| {
			id: string
			email: string
			fullName: string
			role: string
			type: string
			tenantUserCount: number
			tenantUser: Array<{
				id: string
				tenantId: string
				tenantRoleId: string
			}>
	  }
	| {}

export async function getMe(
	userId: string,
): Promise<ServiceResponse<GetMeResponse>> {
	try {
		const user = await UserRepository.getMe(userId)

		if (!user) {
			return HandleServiceResponseCustomError(
				"User not found",
				ResponseStatus.BAD_REQUEST,
			)
		}

		const safeUser: any = exclude(user as any, "password")
		const tenantUser = Array.isArray((user as any)?.tenantUser)
			? (user as any).tenantUser
			: []

		return {
			status: true,
			data: {
				id: safeUser.id,
				email: safeUser.email,
				fullName: safeUser.fullName,
				role: safeUser.role,
				type: safeUser.type,
				tenantUserCount: tenantUser.length,
				tenantUser: tenantUser.map((tu: any) => ({
					id: tu.id,
					tenantId: tu.tenantId,
					tenantRoleId: tu.tenantRoleId,
				})),
			},
		}
	} catch (err) {
		Logger.error(`UserService.getMe`, { error: err })
		return HandleServiceResponseCustomError(
			"Internal Server Error",
			ResponseStatus.INTERNAL_SERVER_ERROR,
		)
	}
}
