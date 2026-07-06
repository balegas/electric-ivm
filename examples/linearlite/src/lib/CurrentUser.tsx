import { createContext, useContext, useMemo, useState } from 'react'
import {
  myMembershipsShapeDef,
  type Project,
  type ProjectMember,
  projectsShapeDef,
  type User,
  usersShapeDef,
} from '../electric'
import { useShapeRows } from './useShape'

// The demo's "logged-in user" is a switchable selection, not real auth. This context loads the live
// roster (users), the project catalog, and the *current* user's memberships, and exposes who is active.
interface CurrentUserState {
  users: User[]
  projects: Project[]
  myMemberships: ProjectMember[] // the current user's project_members rows
  currentUserId: number
  currentUserName: string
  myProjectIds: Set<number>
  projectById: Map<number, Project>
  setCurrentUserId: (id: number) => void
}

const Ctx = createContext<CurrentUserState | null>(null)

export function CurrentUserProvider({ children }: { children: React.ReactNode }): JSX.Element {
  const [currentUserId, setCurrentUserId] = useState(1) // default to the first seeded user (alice)

  const { rows: users } = useShapeRows<User>(usersShapeDef)
  const { rows: projects } = useShapeRows<Project>(projectsShapeDef)
  const { rows: myMemberships } = useShapeRows<ProjectMember>(myMembershipsShapeDef(currentUserId), undefined, [
    currentUserId,
  ])

  const value = useMemo<CurrentUserState>(() => {
    // Shapes carry the primary key as a *string* (TanStack DB collection keys are strings), while non-pk
    // int columns and our `currentUserId` are numbers. Normalize every id to a number so visibility joins
    // (project_id ↔ projects.id, user_id ↔ users.id) compare cleanly across the shape and subset paths.
    const sortedUsers = [...users]
      .map((u) => ({ ...u, id: Number(u.id) }))
      .sort((a, b) => a.id - b.id)
    const sortedProjects = [...projects]
      .map((p) => ({ ...p, id: Number(p.id) }))
      .sort((a, b) => a.id - b.id)
    const normMemberships = myMemberships.map((m) => ({ ...m, id: Number(m.id), project_id: Number(m.project_id) }))
    const currentUserName = sortedUsers.find((u) => u.id === currentUserId)?.name ?? ''
    return {
      users: sortedUsers,
      projects: sortedProjects,
      myMemberships: normMemberships,
      currentUserId,
      currentUserName,
      myProjectIds: new Set(normMemberships.map((m) => m.project_id)),
      projectById: new Map(sortedProjects.map((p) => [p.id, p])),
      setCurrentUserId,
    }
  }, [users, projects, myMemberships, currentUserId])

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>
}

export function useCurrentUser(): CurrentUserState {
  const v = useContext(Ctx)
  if (!v) throw new Error('useCurrentUser must be used within CurrentUserProvider')
  return v
}
